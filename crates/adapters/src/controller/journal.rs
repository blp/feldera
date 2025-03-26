use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fs::{self, remove_file, File, OpenOptions},
    io::{BufReader, BufWriter, Error as IoError, ErrorKind, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::mpsc::{channel, Receiver, Sender},
    thread::{self, JoinHandle},
};

use feldera_adapterlib::{errors::journal::StepError, transport::Step};
use feldera_types::config::InputEndpointConfig;
use rmp_serde::decode::ReadReader;
use rmpv::Value as RmpValue;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::util::write_file_atomically;

pub struct Journal {
    /// Directory name.
    path: PathBuf,
}

impl Journal {
    /// Opens a new journal under `path`.
    pub fn open<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            path: PathBuf::from(path.as_ref()),
        }
    }

    /// Creates a new journal under `path`.
    pub fn create<P>(path: P) -> Result<Self, StepError>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        fs::create_dir_all(path).map_err(|error| StepError::io_error(path, error))?;
        Ok(Self::open(path))
    }

    pub fn read(&self, step: Step) -> Result<StepMetadata, StepError> {
        let path = self.path.join(format!("{step}.bin"));
        let data = fs::read(&path).map_err(|error| StepError::io_error(&path, error))?;
        let record = rmp_serde::decode::from_slice(&data)
            .map_err(|error| StepError::DecodeError { path, error })?;
        Ok(record)
    }

    pub fn write(&self, record: &StepMetadata) -> Result<(), StepError> {
        let path = self.path.join(format!("{}.bin", record.step));
        let data = rmp_serde::encode::to_vec(record).map_err(|error| StepError::EncodeError {
            path: self.path.to_path_buf(),
            error,
        })?;
        write_file_atomically(&path, &data)
            .map_err(|error| StepError::io_error(&self.path, error))?;
        Ok(())
    }

    pub fn truncate(&self) -> Result<(), StepError> {
        let dir = match self.path.read_dir() {
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(self.io_error(error)),
            Ok(dir) => dir,
        };
        for entry in dir {
            entry
                .and_then(|entry| remove_file(entry.path()))
                .map_err(|error| self.io_error(error))?;
        }
        Ok(())
    }

    fn io_error(&self, error: IoError) -> StepError {
        StepError::io_error(&self.path, error)
    }
}

/// A record in the journal, useful for replaying a step.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StepMetadata {
    /// Step number.
    pub step: Step,

    /// Names of input endpoints removed in the step.
    pub remove_inputs: HashSet<String>,

    /// Input endpoints added in the step, with their configurations.
    ///
    /// If a given name is in both `remove_inputs` and `add_inputs`, then the
    /// step replaced an endpoint with the given name by a new, otherwise
    /// unrelated endpoint.
    pub add_inputs: HashMap<String, InputEndpointConfig>,

    /// Logs for the endpoints included in the step.
    ///
    /// A given endpoint is included if it existed before the step and is not in
    /// `remove_inputs`, or if it is included in `add_inputs`.
    pub input_logs: HashMap<String, InputLog>,
}

/// A journal record for a single endpoint for a single step.
///
/// The endpoint's name is the key in [StepMetadata::input_logs].
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct InputLog {
    /// Data for replay.
    ///
    /// This is filled in by input adapters that log actual data records
    /// (e.g. the HTTP and ad hoc query input adapters). For the other adapters,
    /// which only log metadata (such as record offsets), this field is
    /// [RmpValue::Nil].
    pub data: RmpValue,

    /// Metadata for seek and replay.
    ///
    /// This is filled in by input adapters that log metadata (such as record
    /// offsets).
    pub metadata: JsonValue,

    /// Checksums of the input data.
    pub checksums: InputChecksums,
}

/// Input data statistics.
///
/// This allows checking that an input step replayed the same data as the
/// original run.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct InputChecksums {
    /// Number of records.
    pub num_records: u64,

    /// Hash of the records.
    pub hash: u64,
}

/// Checksums for the input endpoints in a step.
///
/// This is a subset of [StepMetadata] that is useful for verifying that an
/// input step replayed the same data as the original run.
#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct StepInputChecksums(
    /// Maps from an input endpoint name to its checksums.
    pub HashMap<String, InputChecksums>,
);

impl From<&HashMap<String, InputLog>> for StepInputChecksums {
    fn from(input_logs: &HashMap<String, InputLog>) -> Self {
        Self(
            input_logs
                .iter()
                .map(|(name, log)| (name.clone(), log.checksums.clone()))
                .collect(),
        )
    }
}

impl From<&StepMetadata> for StepInputChecksums {
    fn from(value: &StepMetadata) -> Self {
        Self::from(&value.input_logs)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tempfile::TempDir;

    use crate::{
        controller::journal::{Journal, ReadResult},
        test::init_test_logger,
    };

    use super::{StepMetadata, StepReader, StepWriter};

    /// Create and write a steps file and then read it back.
    #[test]
    fn test_create() {
        init_test_logger();

        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("journal");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                remove_inputs: HashSet::new(),
                add_inputs: HashMap::new(),
                input_logs: HashMap::new(),
            })
            .collect::<Vec<_>>();

        let mut step_writer = Journal::create(&path).unwrap();
        for step in written_data.iter() {
            step_writer.write(step).unwrap();
            step_writer.wait().unwrap();
        }
        drop(step_writer);

        let mut step_reader = Journal::open(&path).unwrap();
        let mut read_data = Vec::new();
        while let ReadResult::Step {
            reader: new_reader,
            metadata,
        } = step_reader.read().unwrap()
        {
            read_data.push(metadata);
            step_reader = new_reader;
        }
        assert_eq!(written_data, read_data);
    }

    /// Create and write a steps file, then read it back, and continue adding more steps at the end.
    #[test]
    fn test_append() {
        init_test_logger();

        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.bin");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                remove_inputs: HashSet::new(),
                add_inputs: HashMap::new(),
                input_logs: HashMap::new(),
            })
            .collect::<Vec<_>>();

        // Create an empty file and close it immediately.
        // (Thus, this test also checks that we can open and read an empty file.)
        StepWriter::create(&path).unwrap();

        for new_step in 0..10 {
            let mut step_reader = StepReader::open(&path).unwrap();
            let mut read_data = Vec::new();

            // Exactly `new_step` steps should be readable already.
            println!("read steps 0..{new_step}");
            for _ in 0..new_step {
                match step_reader.read().unwrap() {
                    ReadResult::Step {
                        reader: new_reader,
                        metadata,
                    } => {
                        step_reader = new_reader;
                        read_data.push(metadata);
                    }
                    ReadResult::Writer(_) => unreachable!(),
                }
            }
            assert_eq!(&written_data[..new_step], read_data);

            println!("write step {new_step}");
            let mut step_writer = match step_reader.read().unwrap() {
                ReadResult::Step { .. } => unreachable!(),
                ReadResult::Writer(writer) => writer,
            };
            step_writer.write(&written_data[new_step]).unwrap();
        }
    }

    /// Create and write a steps file and then read it back with seeking.
    #[test]
    fn test_seek() {
        init_test_logger();

        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.bin");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                remove_inputs: HashSet::new(),
                add_inputs: HashMap::new(),
                input_logs: HashMap::new(),
            })
            .collect::<Vec<_>>();

        let mut step_writer = StepWriter::create(&path).unwrap();
        for step in written_data.iter() {
            step_writer.write(step).unwrap();
            step_writer.wait().unwrap();
        }
        drop(step_writer);

        for start in 0..10 {
            println!("seek to {start}");
            let step_reader = StepReader::open(&path).unwrap();

            let mut read_data = Vec::new();
            let ReadResult::Step {
                reader: mut step_reader,
                metadata,
            } = step_reader.seek(start).unwrap()
            else {
                unreachable!()
            };
            read_data.push(metadata);
            while let ReadResult::Step {
                reader: new_reader,
                metadata,
            } = step_reader.read().unwrap()
            {
                read_data.push(metadata);
                step_reader = new_reader;
            }
            assert_eq!(&written_data[start as usize..], &read_data);
        }
    }
}
