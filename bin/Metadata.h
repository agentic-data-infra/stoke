// The Kiln cluster stoke's programs use, from --metadata.

#pragma once

#include <cstdlib>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "kiln/client/Client.h"

DECLARE_string(metadata);

namespace stoke {

// A client of the Kiln cluster at --metadata, else $KILN_METADATA, else
// localhost:7470.
inline std::shared_ptr<kiln::client::Client> kilnFromFlags() {
  std::string metadata = FLAGS_metadata;
  if (metadata.empty()) {
    const char* env = std::getenv("KILN_METADATA");
    metadata = env != nullptr ? env : "localhost:7470";
  }
  return std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
      .metadata = kiln::MetadataAddress::parse(metadata)});
}

} // namespace stoke
