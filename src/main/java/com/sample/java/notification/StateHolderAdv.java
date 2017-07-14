package com.sample.java.notification;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

public class StateHolderAdv {
	private final AtomicInteger state = new AtomicInteger();
	private final Set<IStateListener> listeners = new CopyOnWriteArraySet<>();

	public int getState() {
		return state.get();

	}

	public void setState(int newState) {
		StatePublisher publisher = new StatePublisher(
				state.getAndSet(newState), newState);
		if (publisher.oldState != publisher.newState) {
			broadcast(publisher);
		}
	}

	private void broadcast(StatePublisher statePublisher) {

		for (IStateListener listener : listeners) {
			notifySafely(statePublisher, listener);
			removeStateListener(listener);
		}

	}

	private void notifySafely(StatePublisher statePublisher,
			IStateListener listener) {
		try {
			listener.stateChanged(statePublisher);
		} catch (Exception e) {
			//handle exceptions here
		}
		
	}

	public void addStateListener(IStateListener listener) {
		listeners.add(listener);
	}

	public void removeStateListener(IStateListener listener) {
		listeners.remove(listener);
	}

}
