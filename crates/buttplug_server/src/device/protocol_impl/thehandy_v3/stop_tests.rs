// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use super::{TheHandyV3, handy_rpc};
use crate::device::{
  DeviceHandle,
  device_task::{DeviceTaskConfig, DeviceTaskMessage, run_owned_device_task},
  hardware::{
    Hardware,
    HardwareEvent,
    HardwareInternal,
    HardwareReadCmd,
    HardwareReading,
    HardwareSubscribeCmd,
    HardwareUnsubscribeCmd,
    HardwareWriteCmd,
  },
  protocol::ProtocolHandler,
};
use buttplug_core::{errors::ButtplugDeviceError, message::StopCmdV4, util::task::TaskGroup};
use buttplug_server_device_config::{Endpoint, UserDeviceIdentifier, load_protocol_configs};
use futures::future::{self, BoxFuture, FutureExt};
use prost::Message;
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, broadcast, mpsc};
use tokio::time::timeout;
use uuid::Uuid;

struct RecordingHardware {
  writes: Arc<Mutex<Vec<HardwareWriteCmd>>>,
  events: broadcast::Sender<HardwareEvent>,
  started: Arc<Notify>,
  gate: Option<Arc<Semaphore>>,
  fail: bool,
}

impl HardwareInternal for RecordingHardware {
  fn disconnect(&self) -> BoxFuture<'static, Result<(), ButtplugDeviceError>> {
    future::ready(Ok(())).boxed()
  }
  fn event_stream(&self) -> broadcast::Receiver<HardwareEvent> {
    self.events.subscribe()
  }
  fn read_value(
    &self,
    msg: &HardwareReadCmd,
  ) -> BoxFuture<'static, Result<HardwareReading, ButtplugDeviceError>> {
    future::ready(Ok(HardwareReading::new(msg.endpoint(), &[]))).boxed()
  }
  fn subscribe(
    &self,
    _: &HardwareSubscribeCmd,
  ) -> BoxFuture<'static, Result<(), ButtplugDeviceError>> {
    future::ready(Ok(())).boxed()
  }
  fn unsubscribe(
    &self,
    _: &HardwareUnsubscribeCmd,
  ) -> BoxFuture<'static, Result<(), ButtplugDeviceError>> {
    future::ready(Ok(())).boxed()
  }
  fn write_value(
    &self,
    msg: &HardwareWriteCmd,
  ) -> BoxFuture<'static, Result<(), ButtplugDeviceError>> {
    let (writes, started, gate, fail, msg) = (
      self.writes.clone(),
      self.started.clone(),
      self.gate.clone(),
      self.fail,
      msg.clone(),
    );
    async move {
      writes.lock().unwrap().push(msg);
      started.notify_one();
      if let Some(gate) = gate {
        gate.acquire().await.unwrap().forget();
      }
      if fail {
        return Err(ButtplugDeviceError::DeviceCommunicationError(
          "injected write failure".to_owned(),
        ));
      }
      Ok(())
    }
    .boxed()
  }
}

struct Fixture {
  handle: DeviceHandle,
  sender: mpsc::Sender<DeviceTaskMessage>,
  task: tokio::task::JoinHandle<()>,
  writes: Arc<Mutex<Vec<HardwareWriteCmd>>>,
  started: Arc<Notify>,
  feature: Uuid,
  handler: Arc<TheHandyV3>,
}

impl Fixture {
  fn new(gate: Option<Arc<Semaphore>>, fail: bool) -> Self {
    let manager = load_protocol_configs(&None, &None, false)
      .unwrap()
      .finish()
      .unwrap();
    let id = UserDeviceIdentifier::new("test-address", "thehandy-v3", &Some("hw0".to_owned()));
    let definition = manager.device_definition(&id).unwrap();
    let feature = definition.features()[&0].id();
    let writes = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let (events, _) = broadcast::channel(16);
    let hardware = Arc::new(Hardware::new(
      "test-handy",
      "test-address",
      &[Endpoint::Tx],
      &None,
      false,
      Box::new(RecordingHardware {
        writes: writes.clone(),
        events,
        started: started.clone(),
        gate,
        fail,
      }),
    ));
    let handler = Arc::new(TheHandyV3::default());
    let (sender, receiver) = mpsc::channel(16);
    let (device_events, _) = mpsc::channel(16);
    let handle = DeviceHandle::new(
      hardware.clone(),
      handler.clone(),
      definition,
      id,
      vec![],
      sender.clone(),
      device_events,
      Arc::new(AtomicBool::new(false)),
      None,
      TaskGroup::new(),
    );
    let config = DeviceTaskConfig {
      message_gap: Some(Duration::from_millis(50)),
      requires_keepalive: false,
      keepalive_strategy: handler.keepalive_strategy(),
    };
    let task = tokio::spawn(run_owned_device_task(
      hardware,
      handler.clone(),
      config,
      receiver,
    ));
    Self {
      handle,
      sender,
      task,
      writes,
      started,
      feature,
      handler,
    }
  }

  async fn position(&self, position: u32) {
    self
      .sender
      .send(DeviceTaskMessage::fire_and_forget(
        self
          .handler
          .handle_hw_position_with_duration_cmd(0, self.feature, position, 150)
          .unwrap(),
      ))
      .await
      .unwrap();
  }

  async fn close(self) {
    drop(self.handle);
    drop(self.sender);
    timeout(Duration::from_secs(2), self.task)
      .await
      .unwrap()
      .unwrap();
  }
}

fn request(write: &HardwareWriteCmd) -> handy_rpc::Request {
  let message = handy_rpc::RpcMessage::decode(write.data().as_slice()).unwrap();
  assert_eq!(message.r#type, handy_rpc::MessageType::Request as i32);
  let Some(handy_rpc::rpc_message::Message::Request(request)) = message.message else {
    panic!("not an RPC request")
  };
  request
}

#[tokio::test]
async fn handy_stop_replaces_pending_motion_before_ack() {
  let f = Fixture::new(None, false);
  for position in [30, 60, 90] {
    f.position(position).await;
  }
  f.handle.stop(&StopCmdV4::default()).await.unwrap();
  {
    let writes = f.writes.lock().unwrap();
    assert_eq!(
      writes.len(),
      1,
      "Stop must reach hardware and supersede pending motion before its ACK"
    );
    assert!(matches!(
      request(&writes[0]).params,
      Some(handy_rpc::request::Params::RequestHdspStop(_))
    ));
  }
  f.close().await;
}

#[tokio::test]
async fn handy_stop_ack_waits_for_hardware_write() {
  let gate = Arc::new(Semaphore::new(0));
  let f = Fixture::new(Some(gate.clone()), false);
  let mut stop = tokio::spawn(f.handle.stop(&StopCmdV4::default()));
  timeout(Duration::from_millis(200), f.started.notified())
    .await
    .expect("no hardware Stop was written");
  assert!(
    timeout(Duration::from_millis(10), &mut stop).await.is_err(),
    "ACK returned before the GATT write completed"
  );
  gate.add_permits(1);
  stop.await.unwrap().unwrap();
  f.close().await;
}

#[tokio::test]
async fn handy_stop_write_failure_is_not_success() {
  let f = Fixture::new(None, true);
  assert!(
    f.handle.stop(&StopCmdV4::default()).await.is_err(),
    "failed hardware Stop was acknowledged as successful"
  );
  f.close().await;
}

#[tokio::test]
async fn handy_stop_waits_for_in_flight_write_then_discards_pending_positions() {
  let gate = Arc::new(Semaphore::new(0));
  let f = Fixture::new(Some(gate.clone()), false);
  f.position(10).await;
  timeout(Duration::from_secs(1), f.started.notified())
    .await
    .unwrap();
  for position in [30, 60, 90] {
    f.position(position).await;
  }
  let stop = tokio::spawn(f.handle.stop(&StopCmdV4::default()));
  gate.add_permits(1);
  timeout(Duration::from_secs(1), f.started.notified())
    .await
    .unwrap();
  {
    let writes = f.writes.lock().unwrap();
    assert_eq!(writes.len(), 2);
    assert!(matches!(
      request(&writes[0]).params,
      Some(handy_rpc::request::Params::RequestHdspXpTSet(_))
    ));
    assert!(matches!(
      request(&writes[1]).params,
      Some(handy_rpc::request::Params::RequestHdspStop(_))
    ));
  }
  gate.add_permits(1);
  stop.await.unwrap().unwrap();
  f.close().await;
}

#[tokio::test]
async fn handy_stop_timeout_does_not_claim_success() {
  let gate = Arc::new(Semaphore::new(0));
  let f = Fixture::new(Some(gate.clone()), false);
  let result = timeout(Duration::from_secs(2), f.handle.stop(&StopCmdV4::default()))
    .await
    .unwrap();
  assert!(
    result.is_err(),
    "a stalled Stop write must not be acknowledged as successful"
  );
  gate.add_permits(1);
  f.close().await;
}

#[tokio::test]
async fn handy_stop_timeout_includes_waiting_for_queue_space() {
  let gate = Arc::new(Semaphore::new(0));
  let f = Fixture::new(Some(gate.clone()), false);
  f.position(10).await;
  timeout(Duration::from_secs(1), f.started.notified())
    .await
    .unwrap();
  for position in 20..36 {
    f.position(position).await;
  }
  let result = timeout(Duration::from_secs(2), f.handle.stop(&StopCmdV4::default())).await;
  gate.add_permits(32);
  assert!(result.expect("Stop hung waiting for queue space").is_err());
  f.close().await;
}

#[tokio::test]
async fn handy_repeated_idle_stop_still_writes_stop() {
  let f = Fixture::new(None, false);
  f.handle.stop(&StopCmdV4::default()).await.unwrap();
  f.handle.stop(&StopCmdV4::default()).await.unwrap();
  {
    let writes = f.writes.lock().unwrap();
    assert_eq!(writes.len(), 2);
    for write in writes.iter() {
      assert!(write.write_with_response());
      assert_eq!(write.endpoint(), Endpoint::Tx);
      assert!(matches!(
        request(write).params,
        Some(handy_rpc::request::Params::RequestHdspStop(_))
      ));
    }
  }
  f.close().await;
}

#[tokio::test]
async fn handy_stop_respects_output_and_feature_scope() {
  let f = Fixture::new(None, false);
  f.handle
    .stop(&StopCmdV4::new(None, None, true, false))
    .await
    .unwrap();
  assert!(f.writes.lock().unwrap().is_empty());
  assert!(
    f.handle
      .stop(&StopCmdV4::new(None, Some(99), false, true))
      .await
      .is_err()
  );
  assert!(f.writes.lock().unwrap().is_empty());
  f.handle
    .stop(&StopCmdV4::new(None, Some(0), false, true))
    .await
    .unwrap();
  assert_eq!(f.writes.lock().unwrap().len(), 1);
  f.close().await;
}
