use std::{fmt::Debug, panic::Location, time::UNIX_EPOCH};

pub trait LogHelper {
    fn err_log(self, msg: &str) -> Self;
}

impl<R, E: Debug> LogHelper for Result<R, E> {
    #[track_caller]
    fn err_log(self, msg: &str) -> Self {
        match self {
            Err(error) => {
                let caller = Location::caller();
                tracing::error!(
                    ?error,
                    "Error({}:{}), {} ",
                    caller.file(),
                    caller.line(),
                    msg
                );
                Err(error)
            }
            ok => ok,
        }
    }
}

impl<R> LogHelper for Option<R> {
    #[track_caller]
    fn err_log(self, msg: &str) -> Self {
        match self {
            None => {
                let caller = Location::caller();
                tracing::error!("GotNone({}:{}), {} ", caller.file(), caller.line(), msg);
                None
            }
            ok => ok,
        }
    }
}

pub trait PanicHelper {
    type Success;

    fn panic(self, msg: &str) -> Self::Success;
}

impl<R, E> PanicHelper for Result<R, E> {
    type Success = R;

    #[track_caller]
    fn panic(self, msg: &str) -> R {
        match self {
            Ok(v) => v,
            Err(_) => {
                let caller = Location::caller();
                tracing::error!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
                panic!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
            }
        }
    }
}

impl PanicHelper for bool {
    type Success = ();

    #[track_caller]
    fn panic(self, msg: &str) {
        match self {
            true => (),
            false => {
                let caller = Location::caller();
                tracing::error!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
                panic!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
            }
        }
    }
}

impl<R> PanicHelper for Option<R> {
    type Success = R;

    #[track_caller]
    fn panic(self, msg: &str) -> R {
        match self {
            Some(v) => v,
            None => {
                let caller = Location::caller();
                tracing::error!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
                panic!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
            }
        }
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
