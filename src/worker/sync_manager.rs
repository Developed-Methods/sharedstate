use crate::{net::io::SyncIO, state::DeterministicState};

use super::sync_updater::SyncUpdater;

pub struct SyncManager<I: SyncIO, D: DeterministicState> {
    io: I,
    updater: SyncUpdater<I::Address, D>,
}

