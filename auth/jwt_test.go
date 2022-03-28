package auth_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Jwt", func() {
	val := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzYmFkbWluIiwidXNlcl9pZCI6MSwidXNlcl9uYW1lIjoicm9vdCIsInVzZXJfc3RhdHVzIjoxLCJzdWJ1c2VyIjpmYWxzZSwiY3VycmVuY3kiOiJIS0QiLCJwZXJtaXNzaW9uIjoyMTQ3NDgzNjQ3LCJwYXNzd29yZF9leHBpcnkiOjAsImxhc3RfbG9naW4iOjE2MTA0MzcwMjMsImxhc3RfbG9naW5fZnJvbSI6IjE4LjE3Ni42Ny41MiIsInNjb3BlIjoiQURNSU5JU1RSQVRPUiIsInByb2ZpbGVzIjp7fSwidXNlcl90eXBlIjoxLCJ1c2VyX3BhdGgiOiIiLCJ1c2VyX3BhcmVudF9pZCI6MCwiaWF0IjoxNjExODA0NzAwLCJleHAiOjE2MTE4MDgzMDB9.JmruzXycgL-49CyAJ6NHoMB9jPxxO4DBO1GDXDcbBXc"

	Describe("Token length", func() {
		Context("With more than 300", func() {
			It("yes", func() {
				Expect(len(val)).To(Equal(200))
			})
		})
	})
})
