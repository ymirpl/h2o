package water;

/**
 * A UDP Heartbeat packet.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPHeartbeat extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    if( ab._h2o != H2O.SELF ) { // Do not update self-heartbeat object
      // The self-heartbeat is the sole holder of racey cloud-concensus hashes
      // and if we update it here we risk dropping an update.
      ab._h2o._heartbeat = new HeartBeat().read(ab);
      Paxos.doHeartbeat(ab._h2o);
    }
    return ab;
  }

  static void build_and_multicast( H2O cloud, HeartBeat hb ) {
    // Paxos.print_debug("send: heartbeat ",cloud._memset);
    assert hb._cloud_hash != 0; // Set before send, please
    H2O.SELF._heartbeat = hb;
    hb.write(new AutoBuffer(H2O.SELF).putUdp(UDP.udp.heartbeat)).close(false,false);
  }
}
