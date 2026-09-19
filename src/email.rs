//! Magic-link delivery via the Resend HTTP API. Without a key the link
//! is logged instead, which is the local-dev flow.

pub enum Mailer {
    Resend {
        client: reqwest::Client,
        api_key: String,
        from: String,
    },
    /// No LOOKOUT_RESEND_KEY: log the link so dev login still works.
    LogOnly,
}

impl Mailer {
    pub fn new(api_key: Option<String>, from: &str) -> Mailer {
        match api_key {
            Some(api_key) => Mailer::Resend {
                client: reqwest::Client::new(),
                api_key,
                from: from.to_string(),
            },
            None => Mailer::LogOnly,
        }
    }

    /// A buddy-keyed-up notification email; same rate gates as pushes.
    pub async fn send_notification(
        &self,
        to: &str,
        buddy: &str,
        talkgroup: &str,
        history_url: &str,
    ) -> anyhow::Result<()> {
        match self {
            Mailer::LogOnly => {
                tracing::info!(%to, %buddy, "no LOOKOUT_RESEND_KEY; notification email logged only");
                Ok(())
            }
            Mailer::Resend {
                client,
                api_key,
                from,
            } => {
                let subject = format!("{buddy} keyed up on {talkgroup}");
                let text = format!(
                    "{buddy} keyed up on {talkgroup}.\n\n\
                     History: {history_url}\n\n\
                     Manage notification channels from the same page."
                );
                let response = client
                    .post("https://api.resend.com/emails")
                    .bearer_auth(api_key)
                    .json(&serde_json::json!({
                        "from": from,
                        "to": [to],
                        "subject": subject,
                        "text": text,
                    }))
                    .send()
                    .await?;
                if !response.status().is_success() {
                    let status = response.status();
                    anyhow::bail!("resend notification failed: {status}");
                }
                Ok(())
            }
        }
    }

    pub async fn send_magic_link(
        &self,
        to: &str,
        link: &str,
        linking_device: bool,
    ) -> anyhow::Result<()> {
        let action = if linking_device {
            "link your device to this account"
        } else {
            "sign in to DMR Lookout"
        };
        match self {
            Mailer::LogOnly => {
                tracing::info!(%to, %link, "no LOOKOUT_RESEND_KEY; magic link logged only");
                Ok(())
            }
            Mailer::Resend {
                client,
                api_key,
                from,
            } => {
                let text = format!(
                    "Click the link below to {action}:\n\n{link}\n\n\
                     The link expires in 15 minutes and works once. If you \
                     didn't request this, ignore this email."
                );
                let html = format!(
                    "<p>Click the button below to {action}.</p>\
                     <p><a href=\"{link}\" style=\"display:inline-block;padding:12px 20px;\
                     background:#1a73e8;color:#fff;border-radius:6px;text-decoration:none;\
                     font-weight:600\">Sign in to DMR Lookout</a></p>\
                     <p style=\"color:#666;font-size:13px\">The link expires in 15 minutes \
                     and works once. If you didn't request this, ignore this email.</p>"
                );
                let response = client
                    .post("https://api.resend.com/emails")
                    .bearer_auth(api_key)
                    .json(&serde_json::json!({
                        "from": from,
                        "to": [to],
                        "subject": "Sign in to DMR Lookout",
                        "text": text,
                        "html": html,
                    }))
                    .send()
                    .await?;
                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    anyhow::bail!("resend send failed: {status}: {body}");
                }
                Ok(())
            }
        }
    }
}
