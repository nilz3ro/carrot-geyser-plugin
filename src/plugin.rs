use arrayref::array_ref;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::str::FromStr;
use std::sync::mpsc::{channel, Sender};
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
    AccountUpdate {
        pubkey: Pubkey,
        token_account_owner: Pubkey,
        account_data: Vec<u8>,
        write_version: u64,
        slot: u64,
    },
    Shutdown,
}

#[derive(Debug)]
pub struct CarrotPlugin {
    sender: Option<Sender<PulsarMessage>>,
    pulsar_handle: Option<thread::JoinHandle<()>>,
    token_account_filter: Option<TokenAccountFilter>,
}

impl Default for CarrotPlugin {
    fn default() -> Self {
        CarrotPlugin {
            sender: None,
            pulsar_handle: None,
            token_account_filter: None,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StreamNativeOAuth2Config {
    pub issuer_url: String,
    pub credentials_url: String,
    pub audience: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TokenAccountFilter {
    pub mint_address: String,
    pub token_program: String,
    pub topic: String,
}
// topics should be tied to accounts and transactions so we can recieve
#[derive(Deserialize, Serialize, Debug)]
pub struct CarrotPluginConfig {
    pub pulsar_url: String,
    pub streamnative_oauth2: StreamNativeOAuth2Config,
    pub token_account_filter: TokenAccountFilter,
}

pub struct PulsarConfig {
    pub pulsar_url: String,
    pub issuer_url: String,
    pub credentials_url: String,
    pub audience: String,
}

impl CarrotPlugin {
    fn start_pulsar_client(&mut self, config: PulsarConfig) -> PluginResult<()> {
        let (sender, receiver) = channel();
        self.sender = Some(sender);
        let token_account_filter = self
            .token_account_filter
            .as_ref()
            .expect("should access token account filter")
            .topic
            .clone();

        let handle = thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async move {
                let mut builder = Pulsar::builder(config.pulsar_url, TokioExecutor);
                builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(
                    OAuth2Params {
                        issuer_url: config.issuer_url,
                        credentials_url: config.credentials_url, // Absolute path of your downloaded key file
                        audience: Some(config.audience),
                        scope: None,
                    },
                ));

                let pulsar = builder.build().await.expect("should connect to Pulsar");

                // Create producers for each topic
                let mut producer = pulsar
                    .producer()
                    .with_topic(token_account_filter)
                    .build()
                    .await
                    .expect("should create producer");

                producer
                    .check_connection()
                    .await
                    .expect("should be able to connect to pulsar");

                // Process messages from the channel
                while let Ok(msg) = receiver.recv() {
                    match msg {
                        PulsarMessage::AccountUpdate {
                            pubkey,
                            slot,
                            token_account_owner,
                            account_data,
                            write_version,
                        } => {
                            // Create message payload
                            let payload = serde_json::json!({
                                "pubkey": pubkey.to_string(),
                                "slot": slot,
                                "token_account_owner": token_account_owner,
                                "account_data": account_data,
                                "write_version": write_version,
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

        let config: CarrotPluginConfig =
            serde_json::from_reader(&mut config_file).expect("should be able to parse config file");

        let pulsar_config = PulsarConfig {
            pulsar_url: config.pulsar_url,
            issuer_url: config.streamnative_oauth2.issuer_url,
            credentials_url: config.streamnative_oauth2.credentials_url,
            audience: config.streamnative_oauth2.audience,
        };

        self.token_account_filter = Some(config.token_account_filter);
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
        let (pubkey_bytes, owner_pubkey_bytes, account_bytes, write_version) = match account {
            ReplicaAccountInfoVersions::V0_0_1(account_info) => (
                account_info.pubkey,
                account_info.owner,
                account_info.data,
                account_info.write_version,
            ),
            ReplicaAccountInfoVersions::V0_0_2(account_info) => (
                account_info.pubkey,
                account_info.owner,
                account_info.data,
                account_info.write_version,
            ),
            ReplicaAccountInfoVersions::V0_0_3(account_info) => (
                account_info.pubkey,
                account_info.owner,
                account_info.data,
                account_info.write_version,
            ),
        };

        // there must be a cleaner way to compare these
        // bytes without cloning or copying them.
        let token_program_string = self
            .token_account_filter
            .as_ref()
            .expect("should access token account filter")
            .token_program
            .clone();

        let token_program_pubkey =
            Pubkey::from_str(&token_program_string).expect("should parse token program pubkey");

        // Not interested in this program
        // (owner is the owning program here)
        if owner_pubkey_bytes != token_program_pubkey.to_bytes() {
            println!("skipping account update, owner is not token program");
            return Ok(());
        }

        let pubkey = Pubkey::try_from(pubkey_bytes).expect("should construct account pubkey");

        let token_account_mint_bytes = array_ref![account_bytes, 0, 32];
        let token_account_mint_pubkey = Pubkey::new_from_array(*token_account_mint_bytes);

        let mint_address_string = self
            .token_account_filter
            .as_ref()
            .expect("should access token account filter")
            .mint_address
            .clone();

        let expected_mint_address_pubkey =
            Pubkey::from_str(&mint_address_string).expect("should parse mint address pubkey");

        if expected_mint_address_pubkey != token_account_mint_pubkey {
            println!("Mint pubkey does not match expected mint pubkey");
            return Ok(());
        }

        let token_account_owner_bytes = array_ref![account_bytes, 32, 32];
        let token_account_owner_pubkey = Pubkey::new_from_array(*token_account_owner_bytes);

        // Send account update to Pulsar thread if sender exists
        if let Some(sender) = &self.sender {
            if let Err(e) = sender.send(PulsarMessage::AccountUpdate {
                pubkey,
                slot,
                token_account_owner: token_account_owner_pubkey,
                account_data: account_bytes.into(),
                write_version,
            }) {
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
