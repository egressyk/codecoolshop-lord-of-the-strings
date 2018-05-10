package com.codecool.shop.config;

import com.codecool.shop.dao.ProductCategoryDao;
import com.codecool.shop.dao.ProductDao;
import com.codecool.shop.dao.SupplierDao;
import com.codecool.shop.dao.implementation.*;
import com.codecool.shop.model.Product;
import com.codecool.shop.model.ProductCategory;
import com.codecool.shop.model.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;


@WebListener
public class Initializer implements ServletContextListener {

    private static final Logger logger = LoggerFactory.getLogger(ServletContextListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        logger.info("Retrieving DAO instances");
        ProductDao productDataStore = ProductDaoDB.getInstance();
        ProductCategoryDao productCategoryDataStore = ProductCategoryDaoDB.getInstance();
        SupplierDao supplierDataStore = SupplierDaoDB.getInstance();
        logger.info("Removing existing data from tables");
        productDataStore.removeAllProducts();
        productCategoryDataStore.removeAll();
        supplierDataStore.removeAll();

        logger.info("Filling up tables with data");
        //setting up a new supplier
        Supplier mentorBence = new Supplier("Bence", "Mouse-less computer user");
        supplierDataStore.add(mentorBence);
        Supplier mentorZozi = new Supplier("Zozi", "IT-guy philosopher halfling");
        supplierDataStore.add(mentorZozi);
        Supplier mentorRudi = new Supplier("Rudi", "The manifestation of serenity");
        supplierDataStore.add(mentorRudi);
        Supplier mentorDani = new Supplier("Dani", "The designer with a chair");
        supplierDataStore.add(mentorDani);
        Supplier mentorLaci = new Supplier("Laci", "Sports maniac cynic");
        supplierDataStore.add(mentorLaci);

        //setting up a new product category
        ProductCategory material = new ProductCategory("Material", "-", "Written materials in different topics");
        productCategoryDataStore.add(material);
        ProductCategory personalProgramming = new ProductCategory("Personal programming", "-", "Personal live help from mentors");
        productCategoryDataStore.add(personalProgramming);
        ProductCategory specialSkills = new ProductCategory("Special skills", "-", "Personal training in the mentor's special fields of interest");
        productCategoryDataStore.add(specialSkills);
        ProductCategory entertainment = new ProductCategory("Entertainment", "-", "Fun with mentors");
        productCategoryDataStore.add(entertainment);


        //setting up products and printing it
        addWithAllSupplier(supplierDataStore,productDataStore,"Combat Training", 1, "USD", "Private mentoring (consultancy)", personalProgramming);
        addWithAllSupplier(supplierDataStore,productDataStore,"Summon Code Elemental", 3, "USD", "Mentor joins a students' team for one hour", personalProgramming);
        addWithAllSupplier(supplierDataStore,productDataStore,"Circle of Sorcery", 20, "USD", "60 min workshop by a mentor(s) of the chosen topic (mentors will organize it within a months)", personalProgramming);
        addWithAllSupplier(supplierDataStore,productDataStore,"Tome of knowledge", 40, "USD", "Extra material for the current topic (mentors will do it within 2 weeks)", material);
        addWithAllSupplier(supplierDataStore,productDataStore,"Transform mentor", 50, "USD", "Mentor should dress up funny (or pirates) for the specified day (which is at least 2 days ahead)", entertainment);
        addWithAllSupplier(supplierDataStore,productDataStore,"Teleport", 300, "USD", "Teleport the mentor to an off-school location a specified day (which is at least 2 weeks ahead) )", entertainment);
        productDataStore.add(new Product("Guitar Hero", 10, "USD", "Bence will accompany you on guitar in a performace for 45 minutes", specialSkills, mentorBence));
        productDataStore.add(new Product("Departure from the Cave", 10, "USD", "Zozi will enlighten you in a random philosophical topic", specialSkills, mentorZozi));
        productDataStore.add(new Product("Ring the Bell", 10, "USD", "Rudi will be your personal kettle bell trainer for an hour", specialSkills, mentorRudi));
        productDataStore.add(new Product("By Design", 10, "USD", "Dani will brag about one of his creations for an hour", specialSkills, mentorDani));
        productDataStore.add(new Product("Pass the Ball", 10, "USD", "Laci will play any kind of ball game with you for an hour, mocking your miserable skills", specialSkills, mentorLaci));
    }

    private void addWithAllSupplier(SupplierDao supplierDataStore, ProductDao productDataStore, String name, int price, String currency, String desc, ProductCategory category) {
        for (Supplier supplier : supplierDataStore.getAll()){
            productDataStore.add( new Product(name, price, currency, desc, category, supplier));
        }
    }
}
