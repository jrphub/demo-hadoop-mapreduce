package com.sample.java.notification;

public class EmailListener implements IStateListener {

	@Override
	public void stateChanged(StatePublisher publisher) {
		System.out
				.println("Email Listener is activated, because the state changed from :"
						+ publisher.oldState + " to :" + publisher.newState);
	}

}
