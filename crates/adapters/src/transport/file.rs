use super::{
    InputConsumer, InputEndpoint, InputReader, OutputEndpoint, Step, TransportInputEndpoint,
};
use crate::format::StreamingSplitter;
use crate::{Parser, PipelineState};
use anyhow::{bail, Error as AnyError, Result as AnyResult};
use atomic::Atomic;
use crossbeam::sync::{Parker, Unparker};
use feldera_types::program_schema::Relation;
use feldera_types::transport::file::{FileInputConfig, FileOutputConfig};
use std::{
    fs::File,
    io::Write,
    sync::{atomic::Ordering, Arc},
    thread::{sleep, spawn},
    time::Duration,
};

const SLEEP_MS: u64 = 200;

pub(crate) struct FileInputEndpoint {
    config: FileInputConfig,
}

impl FileInputEndpoint {
    pub(crate) fn new(config: FileInputConfig) -> Self {
        Self { config }
    }
}

impl InputEndpoint for FileInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

impl TransportInputEndpoint for FileInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Step,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(FileInputReader::new(
            &self.config,
            consumer,
            parser,
        )?))
    }
}

struct FileInputReader {
    status: Arc<Atomic<PipelineState>>,
    unparker: Option<Unparker>,
}

impl FileInputReader {
    fn new(
        config: &FileInputConfig,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let mut file = File::open(&config.path).map_err(|e| {
            AnyError::msg(format!("Failed to open input file '{}': {e}", config.path))
        })?;

        let parker = Parker::new();
        let unparker = Some(parker.unparker().clone());
        let status = Arc::new(Atomic::new(PipelineState::Paused));
        spawn({
            let follow = config.follow;
            let buffer_size = config.buffer_size_bytes;
            let status = status.clone();
            move || {
                let mut splitter = StreamingSplitter::new(parser.splitter(), buffer_size);
                loop {
                    match status.load(Ordering::Acquire) {
                        PipelineState::Paused => parker.park(),
                        PipelineState::Running => {
                            let n = match splitter.read(&mut file, usize::MAX) {
                                Ok(n) => n,
                                Err(error) => {
                                    consumer.error(true, AnyError::from(error));
                                    break;
                                }
                            };

                            while let Some(chunk) = splitter.next(n == 0 && !follow) {
                                consumer.queue(0, parser.parse(chunk));
                            }
                            if n == 0 {
                                if !follow {
                                    consumer.eoi();
                                    break;
                                }
                                sleep(Duration::from_millis(SLEEP_MS));
                            }
                        }
                        PipelineState::Terminated => break,
                    }
                }
            }
        });

        Ok(Self { status, unparker })
    }

    fn unpark(&self) {
        if let Some(unparker) = &self.unparker {
            unparker.unpark();
        }
    }
}

impl InputReader for FileInputReader {
    fn pause(&self) -> AnyResult<()> {
        // Notify worker thread via the status flag.  The worker may
        // send another buffer downstream before the flag takes effect.
        self.status.store(PipelineState::Paused, Ordering::Release);
        Ok(())
    }

    fn start(&self, _step: Step) -> AnyResult<()> {
        self.status.store(PipelineState::Running, Ordering::Release);

        // Wake up the worker if it's paused.
        self.unpark();
        Ok(())
    }

    fn disconnect(&self) {
        self.status
            .store(PipelineState::Terminated, Ordering::Release);

        // Wake up the worker if it's paused.
        self.unpark();
    }
}

impl Drop for FileInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

pub(crate) struct FileOutputEndpoint {
    file: File,
}

impl FileOutputEndpoint {
    pub(crate) fn new(config: FileOutputConfig) -> AnyResult<Self> {
        let file = File::create(&config.path).map_err(|e| {
            AnyError::msg(format!(
                "Failed to create output file '{}': {e}",
                config.path
            ))
        })?;
        Ok(Self { file })
    }
}

impl OutputEndpoint for FileOutputEndpoint {
    fn connect(
        &mut self,
        _async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> AnyResult<()> {
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        self.file.write_all(buffer)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn push_key(&mut self, _key: &[u8], _val: Option<&[u8]>) -> AnyResult<()> {
        bail!(
            "File output transport does not support key-value pairs. \
This output endpoint was configured with a data format that produces outputs as key-value pairs; \
however the File transport does not support this representation."
        );
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::test::{mock_input_pipeline, wait, DEFAULT_TIMEOUT_MS};
    use csv::WriterBuilder as CsvWriterBuilder;
    use feldera_types::deserialize_without_context;
    use feldera_types::program_schema::Relation;
    use serde::{Deserialize, Serialize};
    use std::{io::Write, thread::sleep, time::Duration};
    use tempfile::NamedTempFile;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
    pub struct TestStruct {
        s: String,
        b: bool,
        i: i64,
    }

    deserialize_without_context!(TestStruct);

    impl TestStruct {
        fn new(s: String, b: bool, i: i64) -> Self {
            Self { s, b, i }
        }
    }

    #[test]
    fn test_csv_file_nofollow() {
        let test_data = [
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];
        let temp_file = NamedTempFile::new().unwrap();

        // Create a transport endpoint attached to the file.
        // Use a very small buffer size for testing.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: file_input
    config:
        path: {:?}
        buffer_size_bytes: 5
format:
    name: csv
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());
        for val in test_data.iter().cloned() {
            writer.serialize(val).unwrap();
        }
        writer.flush().unwrap();

        let (endpoint, consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.start(0).unwrap();
        wait(
            || zset.state().flushed.len() == test_data.len(),
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        for (i, upd) in zset.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
    }

    #[test]
    fn test_csv_file_follow() {
        let test_data = [
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];
        let temp_file = NamedTempFile::new().unwrap();

        // Create a transport endpoint attached to the file.
        // Use a very small buffer size for testing.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: file_input
    config:
        path: {:?}
        buffer_size_bytes: 5
        follow: true
format:
    name: csv
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());

        let (endpoint, consumer, parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

        for _ in 0..10 {
            for val in test_data.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();

            sleep(Duration::from_millis(10));

            // No outputs should be produced at this point.
            assert!(!consumer.state().eoi);

            // Unpause the endpoint, wait for the data to appear at the output.
            endpoint.start(0).unwrap();
            wait(
                || zset.state().flushed.len() == test_data.len(),
                DEFAULT_TIMEOUT_MS,
            )
            .unwrap();
            for (i, upd) in zset.state().flushed.iter().enumerate() {
                assert_eq!(upd.unwrap_insert(), &test_data[i]);
            }
            endpoint.pause().unwrap();

            consumer.reset();
            zset.reset();
        }

        drop(writer);

        consumer.on_error(Some(Box::new(|_, _| {})));
        parser.on_error(Some(Box::new(|_, _| {})));
        temp_file.as_file().write_all(b"xxx\n").unwrap();
        temp_file.as_file().flush().unwrap();

        endpoint.start(0).unwrap();
        wait(
            || {
                let state = parser.state();
                // println!("result: {:?}", state.parser_result);
                state.parser_result.is_some() && !state.parser_result.as_ref().unwrap().is_empty()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();

        assert!(zset.state().flushed.is_empty());

        endpoint.disconnect();
    }
}
