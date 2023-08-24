#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Kv {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub user_meta: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub version: u64,
    #[prost(uint64, tag = "5")]
    pub expires_at: u64,
    #[prost(bytes = "vec", tag = "6")]
    pub meta: ::prost::alloc::vec::Vec<u8>,
    /// Stream id is used to identify which stream the KV came from.
    #[prost(uint32, tag = "10")]
    pub stream_id: u32,
    /// Stream done is used to indicate end of stream.
    #[prost(bool, tag = "11")]
    pub stream_done: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KvList {
    #[prost(message, repeated, tag = "1")]
    pub kv: ::prost::alloc::vec::Vec<Kv>,
    /// alloc_ref used internally for memory management.
    #[prost(uint64, tag = "10")]
    pub alloc_ref: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestChangeSet {
    /// A set of changes that are applied atomically.
    #[prost(message, repeated, tag = "1")]
    pub changes: ::prost::alloc::vec::Vec<ManifestChange>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestChange {
    /// Table ID.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(enumeration = "manifest_change::Operation", tag = "2")]
    pub op: i32,
    /// Only used for CREATE.
    #[prost(uint32, tag = "3")]
    pub level: u32,
    #[prost(uint64, tag = "4")]
    pub key_id: u64,
    #[prost(enumeration = "EncryptionAlgo", tag = "5")]
    pub encryption_algo: i32,
    /// Only used for CREATE Op.
    #[prost(uint32, tag = "6")]
    pub compression: u32,
}
/// Nested message and enum types in `ManifestChange`.
pub mod manifest_change {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Operation {
        Create = 0,
        Delete = 1,
    }
    impl Operation {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Operation::Create => "CREATE",
                Operation::Delete => "DELETE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "CREATE" => Some(Self::Create),
                "DELETE" => Some(Self::Delete),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Checksum {
    /// For storing type of Checksum algorithm used
    #[prost(enumeration = "checksum::Algorithm", tag = "1")]
    pub algo: i32,
    #[prost(uint64, tag = "2")]
    pub sum: u64,
}
/// Nested message and enum types in `Checksum`.
pub mod checksum {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Algorithm {
        Crc32c = 0,
        XxHash64 = 1,
    }
    impl Algorithm {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Algorithm::Crc32c => "CRC32C",
                Algorithm::XxHash64 => "XXHash64",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "CRC32C" => Some(Self::Crc32c),
                "XXHash64" => Some(Self::XxHash64),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataKey {
    #[prost(uint64, tag = "1")]
    pub key_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub iv: ::prost::alloc::vec::Vec<u8>,
    #[prost(int64, tag = "4")]
    pub created_at: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Match {
    #[prost(bytes = "vec", tag = "1")]
    pub prefix: ::prost::alloc::vec::Vec<u8>,
    /// Comma separated with dash to represent ranges "1, 2-3, 4-7, 9"
    #[prost(string, tag = "2")]
    pub ignore_bytes: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EncryptionAlgo {
    Aes = 0,
}
impl EncryptionAlgo {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EncryptionAlgo::Aes => "aes",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "aes" => Some(Self::Aes),
            _ => None,
        }
    }
}
