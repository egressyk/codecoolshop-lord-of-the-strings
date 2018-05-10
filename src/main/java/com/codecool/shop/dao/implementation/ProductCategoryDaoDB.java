package com.codecool.shop.dao.implementation;

import com.codecool.shop.dao.ProductCategoryDao;
import com.codecool.shop.model.Product;
import com.codecool.shop.model.ProductCategory;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProductCategoryDaoDB implements ProductCategoryDao, Queryhandler {

    private static final Logger logger = LoggerFactory.getLogger(Queryhandler.class);
    private String connectionConfigPath = "src/main/resources/connection.properties";
    private static ProductCategoryDaoDB instance = null;

    public static ProductCategoryDaoDB getInstance() {
        if (instance == null) {
            instance = new ProductCategoryDaoDB();
        }
        return instance;
    }

    public ProductCategoryDaoDB(String connectionConfigPath) {
        this.connectionConfigPath = connectionConfigPath;
    }

    public ProductCategoryDaoDB() {
    }

    @Override
    public void add(ProductCategory category) {
        logger.info("Adding new category: {}", category.toString());
        if (category == null) {
            logger.error("Category is null");
            throw new IllegalArgumentException("Null category can not be added.");
        } else if ("".equals(category.getName())){
            logger.error("Category name is empty");
            throw new ValueException("Category must have a name.");
        } else if ("".equals(category.getDepartment())){
            logger.error("Category department is empty");
            throw new ValueException("Category must have a department.");
        } else if ("".equals(category.getDescription())){
            logger.error("Category description is empty");
            throw new ValueException("Category must have a description.");
        }

        String query = "INSERT INTO product_categories (name, description, department) VALUES" +
                " (?, ?, ?);";
        List<Object> parameters = new ArrayList<>();
        parameters.add(category.getName());
        parameters.add(category.getDescription());
        parameters.add(category.getDepartment());
        executeDMLQuery(query, parameters);
        logger.info("Added succesfuly");
    }

    @Override
    public ProductCategory find(int id) {
        logger.info("Searching for category by id: {}", id);
        String query = "SELECT * FROM product_categories WHERE id=?;";
        List<Object> parameters = new ArrayList<>();
        parameters.add(id);
        List<Map<String, Object>> resultList = executeSelectQuery(query, parameters);

        ProductCategory result = null;

        if (resultList.size() == 1) {
            for (Map<String, Object> resultSet : resultList) {
                String name = resultSet.get("name").toString();
                String description = resultSet.get("description").toString();
                String department = resultSet.get("department").toString();
                result = new ProductCategory(name, department, description);
                result.setId(id);
            }
        }
        logger.info("Returning: {}", result.toString());
        return result;
    }

    @Override
    public void remove(int id) {
        logger.info("Removing category by id: {}", id);
        String query = "DELETE FROM product_categories WHERE id=?;";
        List<Object> parameters = new ArrayList<>();
        parameters.add(id);

        String tempQuery = "SELECT * FROM product_categories WHERE id=?;";
        List<Map<String, Object>> resultList = executeSelectQuery(tempQuery, parameters);
        if (resultList.size() == 0){
            logger.info("No category found by id: {}", id);
            throw new IllegalArgumentException("There is no product category with such id in the database.");
        }

        Integer result = executeDMLQuery(query, parameters);
        logger.info("Removed successfully");
    }

    @Override
    public void removeAll() {
        logger.info("Removing all categories");
        String query = "DELETE from product_categories;";
        executeDMLQuery(query);
        logger.info("All categories removed");
    }

    @Override
    public Integer findIdByName(String name) {
        logger.info("Searching for category by name: {}", name);
        String query = "SELECT * FROM product_categories WHERE name=?;";
        List<Object> parameters = new ArrayList<>();
        parameters.add(name);
        List<Map<String, Object>> resultList = executeSelectQuery(query, parameters);

        Integer result = null;
        try {
            result = Integer.parseInt(resultList.get(0).get("id").toString());
        } catch (IndexOutOfBoundsException ex){
            logger.info("No category found by name: {}", name);
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        logger.info("Returning: {}", result.toString());
        return result;
    }

    @Override
    public ProductCategory getDefaultCategory() {
        logger.info("Returning catagory with name 'All'");
        return new ProductCategory("All", "", "");
    }

    @Override
    public List<Product> filterProducts(List<Product> products, ProductCategory category) {
        logger.info("Searching for products by category: {}", category.toString());
        if ((category.toString()).equals(getDefaultCategory().toString())) {
            return products;
        }
        List<Product> temp = new ArrayList<>();
        for (Product product : products) {
            if (product.getProductCategory().toString().equals(category.toString())) {
                temp.add(product);
            }
        }
        logger.info("Retunring filtered products: {}", temp.toString());
        return temp;
    }

    @Override
    public List<ProductCategory> getAll() {
        logger.info("Getting all categories");
        String query = "SELECT * FROM product_categories;";
        List<Map<String, Object>> resultList = executeSelectQuery(query);

        List<ProductCategory> results = new ArrayList<>();

        for (Map<String, Object> resultSet : resultList) {
            String id = resultSet.get("id").toString();
            String name = resultSet.get("name").toString();
            String description = resultSet.get("description").toString();
            String department = resultSet.get("department").toString();
            ProductCategory temp = new ProductCategory(name, department, description);
            temp.setId(Integer.parseInt(id));
            results.add(temp);
        }
        logger.info("Returning all categories: {}", results.toString());
        return results;
    }

    @Override
    public String getConnectionConfigPath() {
        return connectionConfigPath;
    }
}
