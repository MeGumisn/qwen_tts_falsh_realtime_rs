struct Odps<'a> {
    ak: &'a str,
    sk: &'a str,
    endpoint: &'a str,
}

impl<'a> Odps<'a> {
    pub fn new(ak: &'a str, sk: &'a str, endpoint: &'a str) -> Self {
        Odps { ak, sk, endpoint }
    }

    ///
    /// 步骤： sign_request 里calc_auth_str， 加入到header的Authorization
    /// request请求的header示例如下
    /// ```json
    /// {
    ///   "User-Agent": "pyodps/0.12.5.1 CPython/3.13.11 Windows/11",
    ///   "Date": "Wed, 04 Feb 2026 18:40:34 GMT",
    ///   "Authorization": "ODPS **your access id**/20260204/cn/odps/aliyun_v4_request:LJpwqdeznLSpwMCB2XZK0yp00qY="
    /// }
    /// ```
    pub fn get_project(project_name: &str, schema_name: Option<&str>) {}
}
