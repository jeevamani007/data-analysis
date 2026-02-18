"""
SMTP email sending utilities (verification emails).

Uses environment variables configured in .env (see .env.example).
"""

from __future__ import annotations

import smtplib
from email.message import EmailMessage

from config import SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TLS


def send_email_smtp(*, to_email: str, subject: str, html_body: str, text_body: str | None = None) -> bool:
    """
    Send an email using SMTP.

    Returns:
        True if sent, False if skipped/failed.

    Important:
        Email sending must never crash API requests (it can run as a background task).
        When SMTP is not configured, we log and skip.
    """
    if not SMTP_HOST or not SMTP_FROM:
        print("[Email] SMTP not configured (missing SMTP_HOST/SMTP_FROM). Skipping email send.")
        return False
    if SMTP_PASS and not SMTP_USER:
        # If a password is set, most providers require authentication. Fail fast with a clear hint.
        print("[Email] SMTP_PASS is set but SMTP_USER is missing. Skipping email send (would fail auth).")
        return False
    print(f"[Email] Sending email to {to_email} (from {SMTP_FROM}) via {SMTP_HOST}:{SMTP_PORT}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to_email

    # Always include a plain-text body so OTP/link is visible even if HTML is stripped.
    safe_text = text_body or "Please view this email in an HTML-capable client."
    msg.set_content(safe_text)
    msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.ehlo()
            if SMTP_TLS:
                server.starttls()
                server.ehlo()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"[Email] Failed to send email via SMTP: {e}")
        return False


def send_verification_email(*, to_email: str, verify_url: str, otp_code: str | None = None) -> None:
    subject = "Verify your email address"
    otp_html = ""
    otp_text = ""
    if otp_code:
        otp_html = f"""
        <div style="margin:16px 0 0; padding:12px; background:#F8FAFC; border:1px dashed #CBD5E1; border-radius:12px;">
          <div style="font-size:13px; color:#334155; margin-bottom:6px;">Or verify with this OTP code:</div>
          <div style="font-size:22px; letter-spacing:4px; font-weight:800; color:#0F172A;">{otp_code}</div>
          <div style="font-size:12px; color:#64748B; margin-top:6px;">This code expires soon.</div>
        </div>
        """.strip()
        otp_text = f"\nOTP code: {otp_code}\n"

    html = f"""
    <div style="font-family: Inter, Arial, sans-serif; background:#F1F5F9; padding:24px;">
      <div style="max-width:560px; margin:0 auto; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:16px; padding:24px;">
        <h2 style="margin:0 0 8px; color:#0F172A;">Verify your email</h2>
        <p style="margin:0 0 16px; color:#334155;">Click the button below to verify your account.</p>
        <a href="{verify_url}"
           style="display:inline-block; background:linear-gradient(135deg,#0F766E,#0D9488); color:#fff; text-decoration:none; padding:12px 16px; border-radius:12px; font-weight:700;">
           Verify Email
        </a>
        {otp_html}
        <p style="margin:16px 0 0; color:#64748B; font-size:13px;">
          If you did not create this account, you can ignore this email.
        </p>
        <p style="margin:8px 0 0; color:#64748B; font-size:12px; word-break:break-all;">
          Verification link: {verify_url}
        </p>
      </div>
    </div>
    """.strip()

    text = f"Verify your email: {verify_url}{otp_text}"
    sent = send_email_smtp(to_email=to_email, subject=subject, html_body=html, text_body=text)
    if not sent:
        # Dev-friendly: print the verify URL (and OTP) to logs so you can copy/paste.
        print(f"[Email] Verification for {to_email}: {verify_url}" + (f" (OTP: {otp_code})" if otp_code else ""))


def send_password_reset_email(*, to_email: str, otp_code: str) -> None:
    """
    Send an OTP for password reset (forgot password).
    """
    subject = "Password reset OTP"
    html = f"""
    <div style="font-family: Inter, Arial, sans-serif; background:#F1F5F9; padding:24px;">
      <div style="max-width:560px; margin:0 auto; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:16px; padding:24px;">
        <h2 style="margin:0 0 8px; color:#0F172A;">Reset your password</h2>
        <p style="margin:0 0 16px; color:#334155;">Use the OTP below to reset your password. This code expires soon.</p>
        <div style="margin:16px 0 0; padding:12px; background:#F8FAFC; border:1px dashed #CBD5E1; border-radius:12px;">
          <div style="font-size:22px; letter-spacing:4px; font-weight:800; color:#0F172A;">{otp_code}</div>
        </div>
        <p style="margin:16px 0 0; color:#64748B; font-size:13px;">
          If you didn’t request a password reset, you can ignore this email.
        </p>
      </div>
    </div>
    """.strip()
    text = f"Your password reset OTP is: {otp_code}\n\nIf you didn’t request this, ignore this email."
    sent = send_email_smtp(to_email=to_email, subject=subject, html_body=html, text_body=text)
    if not sent:
        print(f"[Email] Password reset OTP for {to_email}: {otp_code}")


