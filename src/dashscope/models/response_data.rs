use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DashScopeResponseData {
    pub request_id: String,
    pub output: Output,
    pub usage: Usage,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Output {
    pub choices: Vec<Choices>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Choices {
    pub finish_reason: String,
    pub index:i32,
    pub message: Message,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message{
    pub content:String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content:Option<String>,
    pub role:String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls:Option<Vec<ToolCall>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ToolCall{
    pub function:Function,
    pub id:String,
    pub index:i32,
    pub r#type:String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Function{
    // 存放参数名->参数值; 如 location:北京
    arguments: HashMap<String, String>,
    name:String
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Usage {
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub output_tokens_details: TokensDetails,
    pub prompt_tokens_details: TokensDetails,
    pub total_tokens: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TokensDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<i32>,
}
