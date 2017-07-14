package com.sample.java.notification;

public class SMSListener implements IStateListener {

	@Override
	public void stateChanged(StatePublisher publisher) {
		System.out
				.println("SMS Listener is activated, because the state changed from :"
						+ publisher.oldState + " to :" + publisher.newState);
	}

}
