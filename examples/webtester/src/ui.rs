use axum::{
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};

const INDEX: &str = include_str!("../static/index.html");
const APP: &str = include_str!("../static/app.js");
const STYLE: &str = include_str!("../static/style.css");

pub fn router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(app))
        .route("/style.css", get(style))
}

async fn index() -> Html<&'static str> {
    Html(INDEX)
}

async fn app() -> Response {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        APP,
    )
        .into_response()
}

async fn style() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLE,
    )
        .into_response()
}
