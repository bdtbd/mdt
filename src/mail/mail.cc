#include "mail/mail.h"
#include <stdio.h>
#include <string.h>
#include <errno.h>

namespace mdt {

int Mail::SendMail(const std::string& to, const std::string& from,
                 const std::string& subject, const std::string& message) {
    int retval = -1;
    FILE *mailpipe = popen("/usr/sbin/sendmail -t ", "w");
    if (mailpipe != NULL) {
        fprintf(mailpipe, "To: %s\n", to.c_str());
        //fprintf(mailpipe, "From: %s\n", from.c_str());
        fprintf(mailpipe, "Subject: %s\n\n", subject.c_str());
        fwrite(message.c_str(), 1, strlen(message.c_str()), mailpipe);
        fwrite(".\n", 1, 2, mailpipe);
        pclose(mailpipe);
        retval = 0;
    } else {
        perror("Failed to invoke sendmail");
    }
    return retval;
}

}
