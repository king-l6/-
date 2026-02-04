#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
发送邮件脚本（支持文本与附件）

通过环境变量配置：
  SMTP_HOST     SMTP服务器地址
  SMTP_PORT     端口(默认465)
  SMTP_USER     登录用户名
  SMTP_PASS     登录密码/授权码
  SMTP_FROM     发件人邮箱
  SMTP_TO       收件人邮箱(多个用逗号分隔)
  SMTP_SUBJECT  主题(可选)
  SMTP_BODY     正文(可选)
  SMTP_BODY_FILE 正文文件路径(可选，优先于 SMTP_BODY)
  SMTP_ATTACH   附件路径(可选)
  SMTP_USE_TLS  使用 STARTTLS(可选，true/false)
"""
import os
import sys
import mimetypes
import smtplib
from email.message import EmailMessage


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"[ERROR] 缺少环境变量: {name}")
        sys.exit(1)
    return value


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def build_message() -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = getenv_required("SMTP_FROM")
    msg["To"] = getenv_required("SMTP_TO")
    msg["Subject"] = os.getenv("SMTP_SUBJECT", "执行结果")

    body_file = os.getenv("SMTP_BODY_FILE")
    if body_file and os.path.exists(body_file):
        with open(body_file, "r", encoding="utf-8") as f:
            body = f.read()
    else:
        body = os.getenv("SMTP_BODY", "")
    msg.set_content(body)

    attach_path = os.getenv("SMTP_ATTACH")
    if attach_path:
        if not os.path.exists(attach_path):
            print(f"[ERROR] 附件不存在: {attach_path}")
            sys.exit(1)
        ctype, encoding = mimetypes.guess_type(attach_path)
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(attach_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype=maintype,
                subtype=subtype,
                filename=os.path.basename(attach_path),
            )
    return msg


def main() -> None:
    host = getenv_required("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "465"))
    user = getenv_required("SMTP_USER")
    password = getenv_required("SMTP_PASS")
    use_tls = parse_bool(os.getenv("SMTP_USE_TLS", "false"))

    msg = build_message()

    try:
        if use_tls or port == 587:
            with smtplib.SMTP(host, port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(user, password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP_SSL(host, port) as smtp:
                smtp.login(user, password)
                smtp.send_message(msg)
        print("[INFO] 邮件发送成功")
    except Exception as e:
        print(f"[ERROR] 邮件发送失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
