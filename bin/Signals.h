// Waiting for SIGINT or SIGTERM in stoke's servers.

#pragma once

#include <csignal>

namespace stoke {

// Blocks SIGINT and SIGTERM in this thread and the threads it starts later,
// so that waitForStopSignal() receives them. Call before starting servers.
inline sigset_t blockStopSignals() {
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  return signals;
}

inline int waitForStopSignal(const sigset_t& signals) {
  int signal = 0;
  sigwait(&signals, &signal);
  return signal;
}

} // namespace stoke
