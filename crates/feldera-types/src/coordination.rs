//! Interface between the coordinator and the pipeline.

use std::{borrow::Cow, collections::BTreeMap, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::{
    config::{InputEndpointConfig, OutputEndpointConfig},
    runtime_status::RuntimeDesiredStatus,
};

/// `/coordination/activate` request, sent by coordinator to pipeline to
/// transition out of [RuntimeDesiredStatus::Coordination].
#[derive(Debug, Serialize, Deserialize)]
pub struct CoordinationActivate {
    pub exchanges: Vec<(SocketAddr, usize)>,
    pub local_address: SocketAddr,
    pub desired_status: RuntimeDesiredStatus,

    /// Input endpoint configuration.
    pub inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig>,

    /// Output endpoint configuration.
    #[serde(default)]
    pub outputs: BTreeMap<Cow<'static, str>, OutputEndpointConfig>,
    // add: checkpoint to start from (if there's no checkpoint to start from
    // then we need to delete all the checkpoints)
}

pub type Step = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationAction {
    /// Wait for instructions from the coordinator.
    Idle,
    /// Wait for a triggering event to occur, such as arrival of a sufficient
    /// amount of data on an input connector.
    Trigger,
    /// Running a step.
    Step,
}

/// `/coordination/status` update, streamed by pipeline to coordinator.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinationStatus {
    /// The step that is running or will run next.
    pub step: Step,
    /// Current action.
    pub action: CoordinationAction,
}

impl CoordinationStatus {
    pub fn new(step: Step, action: CoordinationAction) -> Self {
        Self { step, action }
    }
}

/// `/coordination/request` request, sent by coordinator to pipeline to control
/// running behavior.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinationRequest {
    pub step: Step,
    pub action: CoordinationAction,
}

impl CoordinationRequest {
    pub fn new(step: Step, action: CoordinationAction) -> Self {
        Self { step, action }
    }
}
