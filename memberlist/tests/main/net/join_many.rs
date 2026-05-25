use super::*;

macro_rules! join_many {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_many >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("join_many_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("join_many_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t3_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("join_many_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t4_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("join_many_node_4".into(), $expr);
          t4_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_join_many::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, t2_opts, t3_opts, t4_opts).await;
        });
      }
    }
  };
}

test_mods!(join_many);
