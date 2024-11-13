use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, Result as PluginResult,
    },
    pulsar::{Producer, Pulsar, TokioExecutor},
    solana_program::pubkey::Pubkey,
};

// Message type for our channel
#[derive(Debug)]
enum PulsarMessage {
    AccountUpdate { pubkey: Pubkey, slot: u64 },
    Shutdown,
}

#[derive(Debug)]
pub struct CarrotPlugin {
    sender: Option<Sender<PulsarMessage>>,
    pulsar_handle: Option<thread::JoinHandle<()>>,
}

impl Default for CarrotPlugin {
    fn default() -> Self {
        CarrotPlugin {
            sender: None,
            pulsar_handle: None,
        }
    }
}

pub struct PulsarConfig {
    pub pulsar_url: String,
    pub topic: String,
    pub issuer_url: String,
    pub credentials_url: String,
    pub audience: String,
}

impl CarrotPlugin {
    fn start_pulsar_client(&mut self, config: PulsarConfig) -> PluginResult<()> {
        let (sender, receiver) = channel();
        self.sender = Some(sender);

        let handle = thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async move {
                // Connect to Pulsar
                // let pulsar: Pulsar<_> = Pulsar::builder(pulsar_url, TokioExecutor)
                //     .build()
                //     .await
                //     .expect("Failed to connect to Pulsar");
                let mut builder = Pulsar::builder(config.pulsar_url, TokioExecutor);
                builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(
                    OAuth2Params {
                        issuer_url: config.issuer_url,
                        credentials_url: config.credentials_url, // Absolute path of your downloaded key file
                        audience: Some(config.audience),
                        scope: None,
                    },
                ));

                let pulsar = builder.build().await.expect("Failed to connect to Pulsar");

                // Create producer
                let mut producer = pulsar
                    .producer()
                    .with_topic(config.topic)
                    .build()
                    .await
                    .expect("Failed to create producer");

                producer
                    .check_connection()
                    .await
                    .expect("should be able to connect to pulsar");

                // Process messages from the channel
                while let Ok(msg) = receiver.recv() {
                    match msg {
                        PulsarMessage::AccountUpdate { pubkey, slot } => {
                            // Create message payload
                            let payload = serde_json::json!({
                                "pubkey": pubkey.to_string(),
                                "slot": slot,
                            });

                            // Send to Pulsar
                            if let Err(e) = producer
                                .send_non_blocking(payload.to_string().into_bytes())
                                .await
                            {
                                eprintln!("Failed to send message to Pulsar: {:?}", e);
                            }
                        }
                        PulsarMessage::Shutdown => break,
                    }
                }
            });
        });

        self.pulsar_handle = Some(handle);
        Ok(())
    }
}

impl GeyserPlugin for CarrotPlugin {
    fn name(&self) -> &'static str {
        "carrot-geyser-plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        let mut config_file = File::open(config_file).expect("should be able to open config file");
        let config: Value =
            serde_json::from_reader(&mut config_file).expect("should be able to parse config file");

        // Extract Pulsar configuration from config file
        let pulsar_url = config["pulsar_url"]
            .as_str()
            .expect("pulsar_url must be specified in config")
            .to_string();

        let topic = config["pulsar_topic"]
            .as_str()
            .expect("topic must be specified in config")
            .to_string();

        let issuer_url = config["streamnative_oauth2_issuer_url"]
            .as_str()
            .expect("streamnative_oauth2_issuer_url must be specified in config")
            .to_string();

        let credentials_url = config["streamnative_oauth2_credentials_url"]
            .as_str()
            .expect("streamnative_oauth2_credentials_url must be specified in config")
            .to_string();

        let audience = config["streamnative_oauth2_audience"]
            .as_str()
            .expect("streamnative_oauth2_audience must be specified in config")
            .to_string();

        let pulsar_config = PulsarConfig {
            pulsar_url,
            topic,
            issuer_url,
            credentials_url,
            audience,
        };
        // Start Pulsar client in separate thread
        self.start_pulsar_client(pulsar_config)
    }

    fn on_unload(&mut self) {
        // Send shutdown message if sender exists
        if let Some(sender) = &self.sender {
            let _ = sender.send(PulsarMessage::Shutdown);
        }

        // Wait for Pulsar thread to finish
        if let Some(handle) = self.pulsar_handle.take() {
            let _ = handle.join();
        }
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        _is_startup: bool,
    ) -> PluginResult<()> {
        let pubkey_bytes = match account {
            ReplicaAccountInfoVersions::V0_0_1(account_info) => account_info.pubkey,
            ReplicaAccountInfoVersions::V0_0_2(account_info) => account_info.pubkey,
            ReplicaAccountInfoVersions::V0_0_3(account_info) => account_info.pubkey,
        };

        let pubkey = Pubkey::try_from(pubkey_bytes).unwrap();

        // Send account update to Pulsar thread if sender exists
        if let Some(sender) = &self.sender {
            if let Err(e) = sender.send(PulsarMessage::AccountUpdate { pubkey, slot }) {
                eprintln!("Failed to send account update to Pulsar thread: {:?}", e);
            }
        }

        println!("Account update: pubkey={}, slot={}", pubkey, slot);
        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        false
    }
}
