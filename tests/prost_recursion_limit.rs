// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct NestedMessage {
    #[prost(message, optional, boxed, tag = "1")]
    child: Option<Box<NestedMessage>>,
}

fn nested_message(depth: usize) -> NestedMessage {
    let mut message = NestedMessage { child: None };
    for _ in 0..depth {
        message = NestedMessage {
            child: Some(Box::new(message)),
        };
    }
    message
}

#[test]
fn shallow_protobuf_messages_roundtrip() {
    let message = nested_message(32);
    let encoded = message.encode_to_vec();
    assert_eq!(NestedMessage::decode(encoded.as_slice()).unwrap(), message);
}

#[test]
fn deeply_nested_protobuf_messages_are_rejected() {
    let encoded = nested_message(150).encode_to_vec();
    let error = NestedMessage::decode(encoded.as_slice()).unwrap_err();
    assert!(error.to_string().contains("recursion limit reached"));
}
