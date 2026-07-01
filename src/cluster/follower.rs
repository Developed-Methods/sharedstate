use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    protocol::messages::{SyncRequest, SyncResponse},
    state::deterministic::DeterministicState,
    transport::traits::SyncIOAddress,
};

use super::{
    election::{append_path, valid_remote_leader_path},
    node::Inner,
};

pub(crate) struct FollowConnection<A: SyncIOAddress, D: DeterministicState> {
    pub(crate) remote: A,
    pub(crate) leader_path: Vec<A>,
    pub(crate) to_peer: mpsc::Sender<SyncRequest<A, D>>,
    pub(crate) cancel: CancellationToken,
}

pub(crate) fn spawn_follow_reader<A, D>(
    inner: Arc<Inner<A, D>>,
    target: A,
    mut read: mpsc::Receiver<SyncResponse<A, D>>,
    cancel: CancellationToken,
) where
    A: SyncIOAddress,
    D: DeterministicState,
{
    tokio::spawn(async move {
        loop {
            let msg = tokio::select! {
                _ = cancel.cancelled() => break,
                msg = read.recv() => msg,
            };

            match msg {
                Some(SyncResponse::AuthorityAction(_, action)) => {
                    if let Err(error) = inner.state.lock().await.apply_authority(action).await {
                        tracing::error!(?error, "failed to apply followed authority action");
                        inner.fail(format!("failed to apply followed authority action: {error:?}"));
                        break;
                    }
                }
                Some(SyncResponse::LeaderInfo(info)) => match (info.leader, info.path) {
                    (Some(leader_addr), Some(path))
                        if valid_remote_leader_path(Some(leader_addr), &path, target, inner.address) =>
                    {
                        inner
                            .set_remote_leader_path(
                                leader_addr,
                                Some(info.term),
                                append_path(path, inner.address),
                                Some(target),
                            )
                            .await;
                    }
                    _ => {
                        if inner.clear_follow_to(target).await {
                            inner.clear_remote_leader().await;
                        }
                    }
                },
                Some(SyncResponse::LeaderPath(path)) => {
                    if let Some(leader_addr) = path.first().copied() {
                        if valid_remote_leader_path(Some(leader_addr), &path, target, inner.address) {
                            inner
                                .set_remote_leader_path(
                                    leader_addr,
                                    None,
                                    append_path(path, inner.address),
                                    Some(target),
                                )
                                .await;
                        }
                    }
                }
                Some(SyncResponse::NoPathToLeader) => {
                    if inner.clear_follow_to(target).await {
                        inner.clear_remote_leader().await;
                    }
                }
                Some(SyncResponse::Peers(peers)) => {
                    inner.discover_peers(peers.into_iter()).await;
                }
                Some(SyncResponse::ActionStreamClosed) | None => break,
                _ => {}
            }
        }

        let closed_follow = { inner.control.lock().await.follow.take() };
        if let Some(follow) = closed_follow {
            if follow.remote == target {
                follow.cancel.cancel();
                if let Some(Some(details)) = inner.control.lock().await.peers.get_mut(&target) {
                    details.connected = details.active_connections > 0;
                }
                inner.clear_remote_leader().await;
            } else {
                let _ = inner.control.lock().await.follow.replace(follow);
            }
        }
    });
}
