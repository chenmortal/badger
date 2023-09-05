pub(crate) mod threshold;
	// size of vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
pub(crate) const VLOG_HEADER_SIZE:usize=20;
pub(crate) const MAX_HEADER_SIZE:usize=22;
