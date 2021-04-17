#include "log.hpp"

#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <thread>


namespace mbsc
{

std::mutex Log::s_flushMutex;

Log::Log(Level level)
  : m_level(level)
  , m_mute(false)
{
  switch (level)
  {
  case Level::ERROR: m_os << "ERROR! "; break;
  case Level::WARNING: m_os << "WARNING! "; break;
  case Level::INFO: break;
  case Level::DBG:
#ifdef NDEBUG
    m_mute = true;
#endif
    break;
  }

  if (!m_mute)
  {
    m_os << std::setprecision(12);

    // Print the time in milliseconds since last hour.
    using namespace std::chrono;
    m_os << duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() % (60*60*1000) << " : ";
    m_os << std::this_thread::get_id() << " : ";
  }
}

Log::~Log()
{
  if (!m_mute)
  {
    std::lock_guard<std::mutex> guard(s_flushMutex);
    std::cout << m_os.str() << std::endl;
    if (m_level == Level::ERROR)
      assert(false);
  }
}

}
