# 2026-01-24 SshSeekableZstdVfs/SshSeekableZstdFile SFTP I/O skeleton implementation

- Implemented SshSeekableZstdVfs/SshSeekableZstdFile skeleton for SFTP access supporting both seekable zst and plain files.
- Added fields for SFTP session, file handle, offset, zst detection, etc.
- Implemented seek/read/size/stat/open methods for both zst/plain (dummy logic for now).
- Automatic zst/plain switching by file extension in open (to be improved with header check).
- Added design comments for auto-detection and wrapper switching policy.
- Fixed all syntax errors and ensured successful build.
- At commit, all code compiles and passes existing tests.
