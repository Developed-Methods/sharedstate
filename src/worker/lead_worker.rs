use crate::state::{DeterministicState, LeadUpdater};

pub struct LeadWorker<D: DeterministicState> {
    updater: LeadUpdater<D>
}

impl<D: DeterministicState> LeadWorker<D> {
}
