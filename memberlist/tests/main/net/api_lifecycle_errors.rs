use super::*;

macro_rules! api_lifecycle_errors {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _api_lifecycle_errors >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("api_lifecycle_errors_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_api_lifecycle_errors::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan()).await;
        });
      }
    }
  };
}

test_mods!(api_lifecycle_errors);
