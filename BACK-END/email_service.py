"""
SMTP email sending utilities (verification emails).

Uses environment variables configured in .env (see .env.example).
"""

from __future__ import annotations

import smtplib
from email.message import EmailMessage

from config import SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TLS


def send_email_smtp(*, to_email: str, subject: str, html_body: str, text_body: str | None = None) -> None:
    if not SMTP_HOST or not SMTP_FROM:
        raise RuntimeError("SMTP is not configured. Set SMTP_HOST/SMTP_FROM in .env")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to_email

    if text_body:
        msg.set_content(text_body)
        msg.add_alternative(html_body, subtype="html")
    else:
        msg.set_content(html_body, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        if SMTP_TLS:
            server.starttls()
            server.ehlo()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


def send_verification_email(*, to_email: str, verify_url: str) -> None:
    subject = "Verify your email address"
    html = f"""
    <div style="font-family: Inter, Arial, sans-serif; background:#F1F5F9; padding:24px;">
      <div style="max-width:560px; margin:0 auto; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:16px; padding:24px;">
        <h2 style="margin:0 0 8px; color:#0F172A;">Verify your email</h2>
        <p style="margin:0 0 16px; color:#334155;">Click the button below to verify your account.</p>
        <a href="{verify_url}"
           style="display:inline-block; background:linear-gradient(135deg,#0F766E,#0D9488); color:#fff; text-decoration:none; padding:12px 16px; border-radius:12px; font-weight:700;">
           Verify Email
        </a>
        <p style="margin:16px 0 0; color:#64748B; font-size:13px;">
          If you did not create this account, you can ignore this email.
        </p>
        <p style="margin:8px 0 0; color:#64748B; font-size:12px; word-break:break-all;">
          Verification link: {verify_url}
        </p>
      </div>
    </div>
    """.strip()

    text = f"Verify your email: {verify_url}"
    send_email_smtp(to_email=to_email, subject=subject, html_body=html, text_body=text)


