use std::convert::TryFrom;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Utf8,
    Percent,
    Hex,
    Base64,
}

impl From<Encoding> for &str {
    fn from(value: Encoding) -> Self {
        match value {
            Encoding::Utf8 => "utf8",
            Encoding::Percent => "percent",
            Encoding::Hex => "hex",
            Encoding::Base64 => "base64",
        }
    }
}

impl TryFrom<&str> for Encoding {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "utf8" => Ok(Encoding::Utf8),
            "percent" => Ok(Encoding::Percent),
            "hex" => Ok(Encoding::Hex),
            "base64" => Ok(Encoding::Base64),
            _ => Err(anyhow::anyhow!("Unknown encoding")),
        }
    }
}

impl Encoding {
    pub fn list() -> [&'static str; 4] {
        [
            Encoding::Utf8.into(),
            Encoding::Percent.into(),
            Encoding::Hex.into(),
            Encoding::Base64.into(),
        ]
    }
}

pub fn text_to_binary(value: &str, encoding: Encoding) -> anyhow::Result<Vec<u8>> {
    match encoding {
        Encoding::Utf8 => Ok(value.as_bytes().to_vec()),
        Encoding::Percent => {
            Ok(percent_encoding::percent_decode(value.as_bytes()).collect::<Vec<u8>>())
        }
        Encoding::Hex => Ok(data_encoding::HEXUPPER_PERMISSIVE.decode(value.as_bytes())?),
        Encoding::Base64 => Ok(data_encoding::BASE64.decode(value.as_bytes())?),
    }
}

pub fn binary_to_text(value: &[u8], encoding: Encoding) -> String {
    match encoding {
        Encoding::Utf8 => String::from_utf8_lossy(value).to_string(),
        Encoding::Percent => {
            const SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS.add(b' ');
            let result = percent_encoding::percent_encode(value, SET);
            result.to_string()
        }
        Encoding::Hex => data_encoding::HEXUPPER_PERMISSIVE.encode(value),
        Encoding::Base64 => data_encoding::BASE64.encode(value),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentFormat {
    Json,
    MessagePack,
    Bson,
}

impl From<DocumentFormat> for &str {
    fn from(value: DocumentFormat) -> Self {
        match value {
            DocumentFormat::Json => "json",
            DocumentFormat::MessagePack => "msgpack",
            DocumentFormat::Bson => "bson",
        }
    }
}

impl TryFrom<&str> for DocumentFormat {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "json" => Ok(DocumentFormat::Json),
            "msgpack" => Ok(DocumentFormat::MessagePack),
            "bson" => Ok(DocumentFormat::Bson),
            _ => Err(anyhow::anyhow!("Unknown document format")),
        }
    }
}

impl DocumentFormat {
    pub fn list() -> [&'static str; 3] {
        [
            DocumentFormat::Json.into(),
            DocumentFormat::MessagePack.into(),
            DocumentFormat::Bson.into(),
        ]
    }
}

pub fn binary_to_document(value: &[u8], format: DocumentFormat) -> anyhow::Result<String> {
    match format {
        DocumentFormat::Json => {
            let doc: serde_json::Value = serde_json::from_slice(value)?;
            Ok(doc.to_string())
        }
        DocumentFormat::MessagePack => {
            let doc = rmpv::decode::read_value_ref(&mut std::io::Cursor::new(value))?;
            Ok(doc.to_string())
        }
        DocumentFormat::Bson => {
            let doc = bson::Document::from_reader(&mut std::io::Cursor::new(value))?;
            Ok(doc.to_string())
        }
    }
}
