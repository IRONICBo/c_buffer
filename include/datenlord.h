#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

/// The node ID of the root inode
constexpr static const uint64_t ROOT_ID = 1;

/// Whether to check permission.
/// If fuse mount with `-o default_permissions`, then we should not check
/// permission. Otherwise, we should check permission.
/// TODO: add a feature flag to control this
constexpr static const bool NEED_CHECK_PERM = false;
