#pragma once

#include <sstream>
#include <mutex>

namespace mbsc
{

enum class Level : uint8_t
{
  ERROR,
  WARNING,
  INFO,
  DBG       // Do not use DEBUG name, because it's already defined in some build systems (XCode).
};

class Log
{
  static std::mutex s_flushMutex;

public:
  std::ostringstream m_os;
  Level m_level;
  bool m_mute;

public:
  explicit Log(Level level);

  template <class T> Log & operator << (T const & t)
  {
    if (!m_mute)
      m_os << t;
    return *this;
  }

  ~Log();
};

} // namespace mbsc
