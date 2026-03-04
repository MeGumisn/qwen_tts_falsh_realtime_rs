use serde::{Deserialize, Serialize};
use serde_json::{Error, Value, json};

#[derive(Debug, Serialize, Deserialize)]
struct Tool {
    r#type: String,
    function: Function,
}
impl Tool {
    ///
    /// parameters 元组第一个&str放param的入参名
    pub fn new(
        r#type: &str,
        function_name: &str,
        function_desc: &str,
        parameters: Vec<(&str, FunctionParameters)>,
    ) -> Self {
        let mut params = json!({});
        parameters.into_iter().for_each(|(name, function_params)| {
            params[name] =
                serde_json::to_value(function_params).unwrap_or(Value::String(String::new()));
        });
        let function = Function {
            name: function_name.to_string(),
            description: function_desc.to_string(),
            parameters: params,
        };
        Self {
            r#type: r#type.to_string(),
            function,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Function {
    name: String,
    description: String,
    parameters: Value,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionParameters {
    r#type: String,
    properties: FunctionProperties,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionProperties {
    r#type: String,
    description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    role: String,
    content: String,
    // 回傳結果時需要
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

impl Message {
    pub fn new(role: String, content: String) -> Self {
        Self {
            role,
            content,
            tool_call_id: None,
        }
    }

    pub fn new_with_tool_call(role: String, content: String, tool_call_id: Option<String>) -> Self {
        Self {
            role,
            content,
            tool_call_id,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryMessage {
    user: String,
    bot: String,
}
impl HistoryMessage {
    pub fn new(user: String, bot: String) -> Self {
        Self { user, bot }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Input<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<Vec<HistoryMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages: Option<Vec<Message>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DashScopeRequestBody<'a> {
    model: &'a str,
    input: Input<'a>,
    parameters: Parameters,
}
pub struct DashScopeRequestBodyBuilder<'a> {
    model: &'a str,
    prompt: Option<&'a str>,
    history: Option<Vec<HistoryMessage>>,
    messages: Option<Vec<Message>>,
    parameter: Parameters,
}
impl<'a> DashScopeRequestBodyBuilder<'a> {
    pub fn new(
        model: &'a str,
        prompt: Option<&'a str>,
        history: Option<Vec<HistoryMessage>>,
        messages: Option<Vec<Message>>,
        parameter: Parameters,
    ) -> Self {
        Self {
            model,
            prompt,
            history,
            messages,
            parameter,
        }
    }

    pub fn build(self) -> Result<Value, Error> {
        let input = Input {
            prompt: self.prompt,
            history: self.history,
            messages: self.messages,
        };
        let body = DashScopeRequestBody {
            model: self.model,
            input,
            parameters: self.parameter,
        };
        serde_json::to_value(body)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Parameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_search: Option<bool>,

    /// Enterprise-specific large model id (may be required for some models)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customized_model_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_format: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub incremental_output: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Stop>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub repetition_penalty: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Stop {
    /// A list of stop strings, e.g. ["\n", "STOP"]
    Strings(Vec<String>),
    /// A list of token id sequences, e.g. [[1,2,3],[4,5]]
    IdSequences(Vec<Vec<i32>>),
}
