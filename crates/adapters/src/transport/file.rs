use super::{
    InputConsumer, InputEndpoint, InputReader, InputReaderCommand, InputStep, OutputEndpoint,
    TransportInputEndpoint,
};
use crate::format::StreamingSplitter;
use crate::{Parser, PipelineState};
use anyhow::{bail, Error as AnyError, Result as AnyResult};
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

const SLEEP: Duration = Duration::from_millis(200);

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
        true
    }
}

impl TransportInputEndpoint for FileInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Option<InputStep>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(FileInputReader::new(
            &self.config,
            consumer,
            parser,
        )?))
    }
}

struct StreamingSplitter {
    buffer: Vec<u8>,
    start: u64,
    fragment: Range<usize>,
    fed: usize,
    splitter: Box<dyn Splitter>,
}

impl StreamingSplitter {
    fn new(splitter: Box<dyn Splitter>, buffer_size: usize) -> Self {
        let mut buffer = Vec::new();
        buffer.resize(if buffer_size == 0 { 8192 } else { buffer_size }, 0);
        Self {
            buffer,
            start: 0,
            fragment: 0..0,
            fed: 0,
            splitter,
        }
    }
    fn next(&mut self) -> Option<&[u8]> {
        match self
            .splitter
            .input(&self.buffer[self.fed..self.fragment.end])
        {
            Some(n) => {
                let chunk = &self.buffer[self.fragment.start..self.fed + n];
                self.fed += n;
                self.fragment.start = self.fed;
                Some(chunk)
            }
            None => {
                self.fed = self.fragment.end;
                None
            }
        }
    }
    fn position(&self) -> u64 {
        self.start + self.fragment.start as u64
    }
    fn final_chunk(&mut self) -> Option<&[u8]> {
        if !self.fragment.is_empty() {
            let chunk = &self.buffer[self.fragment.clone()];
            self.fragment.end = self.fragment.start;
            Some(chunk)
        } else {
            None
        }
    }
    fn spare_capacity_mut(&mut self) -> &mut [u8] {
        self.buffer.copy_within(self.fragment.clone(), 0);
        self.start += self.fragment.start as u64;
        self.fed -= self.fragment.start;
        self.fragment = 0..self.fragment.len();
        if self.fragment.len() == self.buffer.len() {
            self.buffer.resize(self.buffer.capacity() * 2, 0);
        }
        &mut self.buffer[self.fragment.len()..]
    }
    fn added_data(&mut self, n: usize) {
        self.fragment.end += n;
    }
    fn read(&mut self, file: &mut File, max: usize) -> Result<usize, IoError> {
        let mut space = self.spare_capacity_mut();
        if space.len() > max {
            space = &mut space[..max];
        }
        let result = file.read(space);
        if let Ok(n) = result {
            println!("read {n} bytes");
            self.added_data(n);
        }
        result
    }
    fn seek(&mut self, offset: u64) {
        self.start = offset;
        self.fragment = 0..0;
        self.fed = 0;
        self.splitter.clear();
    }
}

struct FileInputReader {
    sender: Sender<InputReaderCommand>,
    unparker: Unparker,
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
        let unparker = parker.unparker().clone();
        let (sender, receiver) = channel();
        spawn({
            let follow = config.follow;
            move || {
                if let Err(error) = Self::worker_thread(
                    file,
                    buffer_size,
                    &consumer,
                    parser,
                    parker,
                    receiver,
                    follow,
                ) {
                    consumer.error(true, error);
                }
            }
        });

        Ok(Self { sender, unparker })
    }

    fn worker_thread(
        mut file: File,
        buffer_size: usize,
        consumer: &Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        parker: Parker,
        receiver: Receiver<InputReaderCommand>,
        follow: bool,
    ) -> AnyResult<()> {
        let mut splitter = StreamingSplitter::new(parser.splitter(), buffer_size);

        let mut queue = VecDeque::<(Range<u64>, Box<dyn InputBuffer>)>::new();
        let mut n_queued = 0;
        let mut extending = false;
        let mut eof = false;
        loop {
            for command in receiver.try_iter() {
                match command {
                    InputReaderCommand::Extend => {
                        extending = true;
                    }
                    InputReaderCommand::Queue => {
                        let mut total = 0;
                        let limit = consumer.max_batch_size();
                        let mut range: Option<Range<u64>> = None;
                        while let Some((offsets, mut buffer)) = queue.pop_front() {
                            range = match range {
                                Some(range) => Some(range.start..offsets.end),
                                None => Some(offsets),
                            };
                            total += buffer.len();
                            buffer.flush_all();
                            if total >= limit {
                                break;
                            }
                        }
                        println!("queued {total} records");
                        consumer.extended(
                            total,
                            serde_json::to_value(Metadata {
                                offsets: range.unwrap_or(0..0),
                            })?,
                        );
                    }
                    InputReaderCommand::Seek(metadata) => {
                        let Metadata { offsets } = serde_json::from_value(metadata)?;
                        file.seek(SeekFrom::Start(offsets.end))?;
                    }
                    InputReaderCommand::Replay(metadata) => {
                        let Metadata { offsets } = serde_json::from_value(metadata)?;
                        file.seek(SeekFrom::Start(offsets.start))?;
                        splitter.seek(offsets.start);
                        let mut remainder = (offsets.end - offsets.start) as usize;
                        loop {
                            while let Some(chunk) = splitter.next() {
                                let prev_len = parser.len();
                                consumer.parse_errors(parser.input_chunk(chunk));
                                consumer.buffered(parser.len() - prev_len, chunk.len());
                            }
                            if remainder == 0 {
                                break;
                            }
                            let n = splitter.read(&mut file, remainder)?;
                            if n == 0 {
                                todo!();
                            }
                            remainder -= n;
                        }
                        if let Some(chunk) = splitter.final_chunk() {
                            let prev_len = parser.len();
                            consumer.parse_errors(parser.input_chunk(chunk));
                            consumer.buffered(parser.len() - prev_len, chunk.len());
                        }
                        let num_records = parser.len();
                        parser.take().flush_all();
                        consumer.replayed(num_records);
                    }
                    InputReaderCommand::Disconnect => return Ok(()),
                }
            }

            if !extending || eof || n_queued >= consumer.max_queued_records() {
                parker.park();
                continue;
            }

            let start = splitter.position();
            while let Some(chunk) = splitter.next() {
                let prev_len = parser.len();
                consumer.parse_errors(parser.input_chunk(chunk));
                consumer.buffered(parser.len() - prev_len, chunk.len());
            }
            let n = splitter.read(&mut file, usize::MAX)?;
            if n == 0 {
                if !follow {
                    eof = true;
                    if let Some(chunk) = splitter.final_chunk() {
                        let prev_len = parser.len();
                        consumer.parse_errors(parser.input_chunk(chunk));
                        consumer.buffered(parser.len() - prev_len, chunk.len());
                    }
                    consumer.eoi();
                } else if parser.is_empty() {
                    parker.park_timeout(SLEEP);
                }
            }
            let end = splitter.position();

            if let Some(buffer) = parser.take() {
                n_queued += buffer.len();
                queue.push_back((start..end, buffer));
            }
        }
    }
}

impl InputReader for FileInputReader {
    fn request(&self, command: super::InputReaderCommand) {
        let _ = self.sender.send(command);
        self.unparker.unpark();
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
        endpoint.extend();
        wait(
            || {
                endpoint.queue();
                zset.state().flushed.len() == test_data.len()
            },
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

        endpoint.extend();

        for _ in 0..10 {
            for val in test_data.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();

            sleep(Duration::from_millis(10));

            // No outputs should be produced at this point.
            assert!(!consumer.state().eoi);

            // Unpause the endpoint, wait for the data to appear at the output.
            wait(
                || {
                    endpoint.queue();
                    zset.state().flushed.len() == test_data.len()
                },
                DEFAULT_TIMEOUT_MS,
            )
            .unwrap();
            for (i, upd) in zset.state().flushed.iter().enumerate() {
                assert_eq!(upd.unwrap_insert(), &test_data[i]);
            }

            consumer.reset();
            zset.reset();
        }

        drop(writer);

        consumer.on_error(Some(Box::new(|_, _| {})));
        parser.on_error(Some(Box::new(|_, _| {})));
        temp_file.as_file().write_all(b"xxx\n").unwrap();
        temp_file.as_file().flush().unwrap();

        wait(
            || {
                endpoint.queue();
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

#[derive(Serialize, Deserialize)]
struct Metadata {
    offsets: Range<u64>,
}
