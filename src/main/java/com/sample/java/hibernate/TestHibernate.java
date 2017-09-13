package com.sample.java.hibernate;

import org.hibernate.Session;

/**
 * @author jrp
 * 
 * https://howtodoinjava.com/hibernate/how-to-define-association-mappings-between-hibernate-entities/
 *
 */
public class TestHibernate {
	public static void main(String[] args) {
		Session sessionOne = HibernateUtil.getSessionFactory().openSession();
		sessionOne.beginTransaction();

		// Create new Employee object
		Employee emp = new Employee();
		emp.setName("jrp");
		emp.setAge(25);

		// Save employee
		sessionOne.save(emp);
		// store the employee id generated for future use
		Integer empId = emp.getId();
		sessionOne.getTransaction().commit();
		sessionOne.close();
		/************************************************************************/

		// Let's open a new session to test load() methods
		Session sessionTwo = HibernateUtil.getSessionFactory().openSession();
		sessionTwo.beginTransaction();

		// first load() method example
		Employee emp1 = (Employee) sessionTwo.load(Employee.class, empId);
		System.out.println(emp1.getName() + " - " + emp1.getAge());

		// Let's verify the entity name
		System.out.println(sessionTwo.getEntityName(emp1));

		sessionTwo.getTransaction().commit();
		sessionTwo.close();
		/************************************************************************/

		Session sessionThree = HibernateUtil.getSessionFactory().openSession();
		sessionThree.beginTransaction();

		// second load() method example
		Employee emp2 = (Employee) sessionThree.load(
				"com.sample.java.hibernate.Employee", empId);
		System.out.println(emp2.getName() + " - " + emp2.getAge());

		sessionThree.getTransaction().commit();
		sessionThree.close();
		/************************************************************************/

		Session sessionFour = HibernateUtil.getSessionFactory().openSession();
		sessionFour.beginTransaction();

		// third load() method example
		Employee emp3 = new Employee();
		sessionFour.load(emp3, empId);
		System.out.println(emp3.getName() + " - " + emp3.getAge());

		sessionFour.getTransaction().commit();
		sessionFour.close();
		System.exit(0);
	}
}