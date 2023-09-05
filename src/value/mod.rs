pub(crate) mod threshold;
	// size of vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
pub(crate) const vlogHeaderSize:usize=20;
