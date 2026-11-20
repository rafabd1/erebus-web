use axum::Json;
use axum::http::StatusCode;
use axum::http::header::RETRY_AFTER;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("not found")]
    NotFound,
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("{0}")]
    InvalidInput(String),
    #[error("runtime is busy")]
    Busy,
    #[error("storage error: {0}")]
    Storage(#[from] anyhow::Error),
    #[error("runtime task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Self::Busy => StatusCode::TOO_MANY_REQUESTS,
            Self::Storage(_) | Self::Join(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut response = (
            status,
            Json(ErrorBody {
                error: match &self {
                    Self::Storage(_) | Self::Join(_) => "internal runtime error".into(),
                    _ => self.to_string(),
                },
            }),
        )
            .into_response();
        if matches!(self, Self::Busy) {
            response
                .headers_mut()
                .insert(RETRY_AFTER, "1".parse().expect("static header is valid"));
        }
        response
    }
}
