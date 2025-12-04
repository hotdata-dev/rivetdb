use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// API error with HTTP status code
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    pub code: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            code: "BAD_REQUEST".to_string(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            code: "NOT_FOUND".to_string(),
        }
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            code: "INTERNAL_SERVER_ERROR".to_string(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            code: "CONFLICT".to_string(),
        }
    }

    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
            code: "BAD_GATEWAY".to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "error": {
                "message": self.message,
                "code": self.code,
            }
        }));

        (self.status, body).into_response()
    }
}

/// Convert anyhow::Error to ApiError
impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        // For now, treat all anyhow errors as internal server errors
        // TODO: Parse error messages to classify as 400 vs 500
        ApiError::internal_error(err.to_string())
    }
}
