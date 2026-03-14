use crate::config::TlsMode;
use rustls_pki_types::pem::PemObject;
use std::path::Path;
use std::sync::Arc;
use tokio_postgres::NoTls;

/// Connect to Postgres with appropriate TLS based on the configured mode.
pub async fn pg_connect(
    conn_string: &str,
    tls_mode: &TlsMode,
    ca_cert_path: Option<&Path>,
) -> anyhow::Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    match tls_mode {
        TlsMode::Disable => {
            let (client, conn) = tokio_postgres::connect(conn_string, NoTls).await?;
            let handle = tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::error!("Postgres connection error: {}", e);
                }
            });
            Ok((client, handle))
        }
        _ => {
            let config = build_rustls_config(tls_mode, ca_cert_path)?;
            let connector = tokio_postgres_rustls::MakeRustlsConnect::new(config);
            let (client, conn) = tokio_postgres::connect(conn_string, connector).await?;
            let handle = tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::error!("Postgres connection error: {}", e);
                }
            });
            Ok((client, handle))
        }
    }
}

/// Create a `deadpool_postgres::Pool` with appropriate TLS.
pub fn create_pg_pool(
    cfg: deadpool_postgres::Config,
    tls_mode: &TlsMode,
    ca_cert_path: Option<&Path>,
) -> anyhow::Result<deadpool_postgres::Pool> {
    match tls_mode {
        TlsMode::Disable => Ok(cfg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)?),
        _ => {
            let config = build_rustls_config(tls_mode, ca_cert_path)?;
            let connector = tokio_postgres_rustls::MakeRustlsConnect::new(config);
            Ok(cfg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), connector)?)
        }
    }
}

fn build_rustls_config(
    tls_mode: &TlsMode,
    ca_cert_path: Option<&Path>,
) -> anyhow::Result<rustls::ClientConfig> {
    match tls_mode {
        TlsMode::Disable => unreachable!(),
        TlsMode::Prefer | TlsMode::Require => Ok(rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
            .with_no_client_auth()),
        TlsMode::VerifyCa | TlsMode::VerifyFull => {
            let root_store = build_root_store(ca_cert_path)?;
            Ok(rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth())
        }
    }
}

fn build_root_store(ca_cert_path: Option<&Path>) -> anyhow::Result<rustls::RootCertStore> {
    let mut root_store = rustls::RootCertStore::empty();
    if let Some(ca_path) = ca_cert_path {
        let pem_data = std::fs::read(ca_path)?;
        let certs: Vec<_> = rustls_pki_types::CertificateDer::pem_slice_iter(&pem_data)
            .collect::<Result<_, _>>()?;
        for cert in certs {
            root_store.add(cert)?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    Ok(root_store)
}

/// Certificate verifier that accepts any certificate (for `require` / `prefer` mode).
#[derive(Debug)]
struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}
