package com.sample.java.notification;

import java.util.HashSet;
import java.util.Set;

public class StateHolder {
	private int state;
	private final Set<IStateListener> listeners = new HashSet<>();

	public int getState() {
		synchronized (listeners) {
			return state;
		}

	}

	public void setState(int state) {
		StatePublisher publisher;
		synchronized (listeners) {
			publisher = new StatePublisher(this.state, state);
			this.state = state;
		}
		if (publisher.oldState != publisher.newState) {
			broadcast(publisher);
		}
	}

	private void broadcast(StatePublisher statePublisher) {
		Set<IStateListener> snapshot;
		synchronized (listeners) {
			snapshot = new HashSet<IStateListener>(this.listeners);
		}

		for (IStateListener listener : snapshot) {
			listener.stateChanged(statePublisher);
			removeStateListener(listener);
			// This will cause Exception in thread "main"
			// java.util.ConcurrentModificationException,
			// if snapshot is not used
		}
		
	}

	public void addStateListener(IStateListener listener) {
		synchronized (listeners) {
			listeners.add(listener);
		}
	}

	public void removeStateListener(IStateListener listener) {
		synchronized (listeners) {
			listeners.remove(listener);
		}
	}

}
