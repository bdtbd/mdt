#ifndef _MDT_MAIL_H
#define _MDT_MAIL_H
#include <string>

namespace mdt {
class Mail {
public:
    int SendMail(const std::string& to, const std::string& from,
                 const std::string& subject, const std::string& message);
};
}
#endif
