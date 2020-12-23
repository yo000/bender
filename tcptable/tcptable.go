/*
Copyright yo000 <johan@nosd.in> 2020

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// From tcp_table(5):
// PROTOCOL DESCRIPTION
//       The TCP map class implements a very simple protocol: the client sends a
//       request, and the server sends one reply. Requests and replies are  sent
//       as  one  line of ASCII text, terminated by the ASCII newline character.
//       Request and reply parameters (see below) are separated by whitespace. 
package tcpTable

import (
	"net"

	"github.com/pinterest/bender"
)

// REQUEST FORMAT
//       The tcp_table protocol supports only the lookup request.   The  request
//       has the following form:
//
//       get SPACE key NEWLINE
//              Look up data under the specified key.
//
//       Postfix  will  not  generate  partial  search keys such as domain names
//       without one or more subdomains, network addresses without one  or  more
//       least-significant  octets,  or  email  addresses without the localpart,
//       address extension or domain portion. This behavior is also  found  with
//       cidr:, pcre:, and regexp: tables.
type TcpTableRequest struct {
	EndPoint string
	Request  string
}


// ResponseValidator validates a TCP response.
type ResponseValidator func(request interface{}, resp []byte) error

// CreateExecutor creates an HTTP request executor.
func CreateExecutor(cnx *net.Conn, responseValidator ResponseValidator) bender.RequestExecutor {
	if cnx == nil {
		cnx = &net.Conn{}
	}

	return func(_ int64, request interface{}) (interface{}, error) {
		req := request.(*TcpTableRequest)
		cnx, err = net.Dial("tcp", req.EndPoint)
		if err != nil {
			return nil, err
		}
		resp, err := cnx.Write(append([]byte(req.Request), '\n'))
		if err != nil {
			return nil, err
		}
		err = responseValidator(request, resp)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
}

