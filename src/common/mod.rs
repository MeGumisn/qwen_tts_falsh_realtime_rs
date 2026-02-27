pub mod logging;
pub mod errors;

pub fn get_platform_info() -> (String, String) {
    #[cfg(target_os = "windows")]{
        // 1. 獲取 Platform (Windows 11 10.0.22621)
        let version = windows_version::OsVersion::current();

        // 構造 10.0.22621 格式
        let full_ver = format!("{}.{}.{}", version.major, version.minor, version.build);

        // 根據 Build 號判斷是 Win10 還是 Win11 (22000 以上為 Win11)
        let os_name = if version.build >= 22000 {
            "Windows-11"
        } else {
            "Windows-10"
        };

        (os_name.to_string(), full_ver)
    }
    #[cfg(target_os = "linux")]{
        let os_name = sys_info::os_type().unwrap_or("Unknown".to_string());
        let os_version = sys_info::os_release().unwrap_or("Unknown Version".to_string());
        (os_name, os_version)
    }
}

pub fn get_processor_info() -> String {
    let cpuid = raw_cpuid::CpuId::new();
    if let Some(fms) = cpuid.get_feature_info() {
        let arch = std::env::consts::ARCH; // 或透過環境變數 std::env::consts::ARCH 獲取
        let vendor = cpuid
            .get_vendor_info()
            .map(|v| v.to_string())
            .unwrap_or("Unknown".to_string());
        format!(
            "{} Family {} Model {} Stepping {}, {}",
            arch,
            fms.family_id(),
            fms.model_id(),
            fms.stepping_id(),
            vendor
        )
    } else {
        "Unknown Processor".to_string()
    }
}
