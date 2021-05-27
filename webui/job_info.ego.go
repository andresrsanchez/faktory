// Generated by ego.
// DO NOT EDIT

//line job_info.ego:1

package webui

import "fmt"
import "html"
import "io"
import "context"

import (
	"net/http"

	"github.com/contribsys/faktory/client"
)

func ego_job_info(w io.Writer, req *http.Request, job *client.Job) {

//line job_info.ego:12
	_, _ = io.WriteString(w, "\n\n<header>\n  <h3>")
//line job_info.ego:14
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Job"))))
//line job_info.ego:14
	_, _ = io.WriteString(w, "</h3>\n</header>\n\n<div class=\"table-responsive\">\n  <table class=\"table table-bordered table-striped table-light\">\n    <tbody>\n      <tr>\n        <th>JID</th>\n        <td>\n          <code>")
//line job_info.ego:23
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Jid)))
//line job_info.ego:23
	_, _ = io.WriteString(w, "</code>\n        </td>\n      </tr>\n      <tr>\n        <th>")
//line job_info.ego:27
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Job"))))
//line job_info.ego:27
	_, _ = io.WriteString(w, "</th>\n        <td>\n          <code>")
//line job_info.ego:29
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Type)))
//line job_info.ego:29
	_, _ = io.WriteString(w, "</code>\n        </td>\n      </tr>\n      <tr>\n        <th>")
//line job_info.ego:33
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Arguments"))))
//line job_info.ego:33
	_, _ = io.WriteString(w, "</th>\n        <td>\n          <code class=\"code-wrap\">\n            <!-- We don't want to truncate any job arguments when viewing a single job's status page -->\n            <div class=\"args-extended\">")
//line job_info.ego:37
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(displayFullArgs(job.Args))))
//line job_info.ego:37
	_, _ = io.WriteString(w, "</div>\n          </code>\n        </td>\n      </tr>\n      <tr>\n        <th>")
//line job_info.ego:42
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "CreatedAt"))))
//line job_info.ego:42
	_, _ = io.WriteString(w, "</th>\n        <td>")
//line job_info.ego:43
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(relativeTime(job.CreatedAt))))
//line job_info.ego:43
	_, _ = io.WriteString(w, "</td>\n      </tr>\n      ")
//line job_info.ego:45
	if job.At != "" {
//line job_info.ego:46
		_, _ = io.WriteString(w, "\n        <tr>\n          <th>")
//line job_info.ego:47
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Scheduled"))))
//line job_info.ego:47
		_, _ = io.WriteString(w, "</th>\n          <td>")
//line job_info.ego:48
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(relativeTime(job.At))))
//line job_info.ego:48
		_, _ = io.WriteString(w, "</td>\n        </tr>\n      ")
//line job_info.ego:50
	}
//line job_info.ego:51
	_, _ = io.WriteString(w, "\n      <tr>\n        <th>")
//line job_info.ego:52
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Queue"))))
//line job_info.ego:52
	_, _ = io.WriteString(w, "</th>\n        <td>\n          <a href=\"")
//line job_info.ego:54
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(root(req))))
//line job_info.ego:54
	_, _ = io.WriteString(w, "/queues/")
//line job_info.ego:54
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Queue)))
//line job_info.ego:54
	_, _ = io.WriteString(w, "\">")
//line job_info.ego:54
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Queue)))
//line job_info.ego:54
	_, _ = io.WriteString(w, "</a>\n        </td>\n      </tr>\n      <tr>\n        <th>")
//line job_info.ego:58
	_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Enqueued"))))
//line job_info.ego:58
	_, _ = io.WriteString(w, "</th>\n        <td>\n          ")
//line job_info.ego:60
	enq := job.EnqueuedAt
	if enq != "" {
//line job_info.ego:61
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(relativeTime(enq))))
//line job_info.ego:62
	} else {
//line job_info.ego:63
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "NotYetEnqueued"))))
//line job_info.ego:64
	}
//line job_info.ego:65
	_, _ = io.WriteString(w, "\n        </td>\n      </tr>\n      ")
//line job_info.ego:67
	if job.Custom != nil {
//line job_info.ego:68
		_, _ = io.WriteString(w, "\n        <tr>\n          <th>")
//line job_info.ego:69
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Custom"))))
//line job_info.ego:69
		_, _ = io.WriteString(w, "</th>\n          <td>\n            ")
//line job_info.ego:71
		for k, v := range job.Custom {
//line job_info.ego:72
			_, _ = io.WriteString(w, "\n              <code>")
//line job_info.ego:72
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(k)))
//line job_info.ego:72
			_, _ = io.WriteString(w, ": ")
//line job_info.ego:72
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(fmt.Sprintf("%#v", v))))
//line job_info.ego:72
			_, _ = io.WriteString(w, "</code><br/>\n            ")
//line job_info.ego:73
		}
//line job_info.ego:74
		_, _ = io.WriteString(w, "\n          </td>\n        </tr>\n      ")
//line job_info.ego:76
	}
//line job_info.ego:77
	if job.Failure != nil {
//line job_info.ego:78
		_, _ = io.WriteString(w, "\n        <tr>\n          <th>")
//line job_info.ego:79
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "RetryCount"))))
//line job_info.ego:79
		_, _ = io.WriteString(w, "</th>\n          <td>")
//line job_info.ego:80
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Failure.RetryCount)))
//line job_info.ego:80
		_, _ = io.WriteString(w, "</td>\n        </tr>\n        <tr>\n          <th>")
//line job_info.ego:83
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "OriginallyFailed"))))
//line job_info.ego:83
		_, _ = io.WriteString(w, "</th>\n          <td>")
//line job_info.ego:84
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(relativeTime(job.Failure.FailedAt))))
//line job_info.ego:84
		_, _ = io.WriteString(w, "</td>\n        </tr>\n        <tr>\n          <th>")
//line job_info.ego:87
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "NextRetry"))))
//line job_info.ego:87
		_, _ = io.WriteString(w, "</th>\n          <td>")
//line job_info.ego:88
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(relativeTime(job.Failure.NextAt))))
//line job_info.ego:88
		_, _ = io.WriteString(w, "</td>\n        </tr>\n      ")
//line job_info.ego:90
	}
//line job_info.ego:91
	_, _ = io.WriteString(w, "\n    </tbody>\n  </table>\n</div>\n")
//line job_info.ego:94
}

var _ fmt.Stringer
var _ io.Reader
var _ context.Context
var _ = html.EscapeString
