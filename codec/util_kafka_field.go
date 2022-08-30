// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package codec

func generateSaslAuthUsernamePwdBytes(username string, password string) []byte {
	idx := 1
	bytes := make([]byte, len(username)+len(password)+2)
	copy(bytes[idx:idx+len(username)], username)
	idx += len(username) + 1
	copy(bytes[idx:idx+len(password)], password)
	return bytes
}

func readSaslUsernamePwdByAuthBytes(bytes []byte, idx int) (string, string) {
	usernameIdx := idx
	totalLength := idx + len(bytes)
	for i := idx + 1; i < totalLength; i++ {
		usernameIdx = i
		if bytes[i] == 0 {
			break
		}
	}
	return string(bytes[idx+1 : usernameIdx]), string(bytes[usernameIdx+1 : totalLength])
}

func readTaggedField(bytes []byte, idx int) int {
	if bytes[idx] != 0 {
		panic("not supported tagged field yet")
	}
	return idx + 1
}

func putTaggedField(bytes []byte, idx int) int {
	bytes[idx] = 0
	return idx + 1
}
