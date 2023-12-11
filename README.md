# TransportController

## 概要
入力されたメッセージの圧縮を行い、アウトプットに送信する。
また、メッセージバッファリングを用いた転送制御を行う。

バッファリングの仕組み（「transportcontrol」がtrueの場合のみ有効）

「unitkey」で指定されたプロパティごとにメッセージをグループ化してバッファリングする。

バッファされたメッセージのサイズが「sendsizemax」で指定されたサイズを超えた場合、
バッファされているメッセージを1つのメッセージとして送信する。（「bandwidthcontrol」がfalseの場合のみ）

「sendcycle」の時間が経過した場合、バッファされているメッセージを1つのメッセージとして送信する。

## Quick Start

## Feedback
お気づきの点があれば、ぜひIssueにてお知らせください。

## LICENSE
This project is licensed under the MIT License, see the LICENSE file for details
