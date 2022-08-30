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

// This file is for kafka code bool type. Format method as alpha order.

func readAllowAutoTopicCreation(bytes []byte, idx int) (bool, int) {
	return readBool(bytes, idx)
}

func putAllowAutoTopicCreation(bytes []byte, idx int, x bool) int {
	return putBool(bytes, idx, x)
}

func readIncludeClusterAuthorizedOperations(bytes []byte, idx int) (bool, int) {
	return readBool(bytes, idx)
}

func putIncludeClusterAuthorizedOperations(bytes []byte, idx int, x bool) int {
	return putBool(bytes, idx, x)
}

func readIncludeTopicAuthorizedOperations(bytes []byte, idx int) (bool, int) {
	return readBool(bytes, idx)
}

func putIncludeTopicAuthorizedOperations(bytes []byte, idx int, x bool) int {
	return putBool(bytes, idx, x)
}
