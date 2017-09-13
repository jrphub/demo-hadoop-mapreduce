package com.sample.java.hibernate;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

public class App {

	public static void main(String[] args) {

		Employee em1 = new Employee("Mary Smith", 25);
		Employee em2 = new Employee("John Aces", 32);
		Employee em3 = new Employee("Ian Young", 29);

		System.out.println(" =======CREATE =======");
		HibernateUtil.create(em1);
		HibernateUtil.create(em2);
		HibernateUtil.create(em3);
		System.out.println(" =======READ =======");
		List<Employee> ems1 = HibernateUtil.read();
		for (Employee e : ems1) {
			System.out.println(e.toString());
		}
		System.out.println(" =======UPDATE =======");
		em1.setAge(44);
		em1.setName("Mary Rose");
		HibernateUtil.update(em1);
		System.out.println(" =======READ =======");
		List<Employee> ems2 = HibernateUtil.read();
		for (Employee e : ems2) {
			System.out.println(e.toString());
		}
		System.out.println(" =======DELETE ======= ");
		HibernateUtil.delete(em2.getId());
		System.out.println(" =======READ =======");
		List<Employee> ems3 = HibernateUtil.read();
		for (Employee e : ems3) {
			System.out.println(e.toString());
		}
		System.out.println(" =======DELETE ALL ======= ");
		HibernateUtil.deleteAll();
		System.out.println(" =======READ =======");
		List<Employee> ems4 = HibernateUtil.read();
		for (Employee e : ems4) {
			System.out.println(e.toString());
		}
		System.exit(0);
	}

}
