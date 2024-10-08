package com.javarush;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javarush.config.HibernateUtil;
import com.javarush.config.RedisUtil;
import com.javarush.dao.CityDAO;
import com.javarush.dao.CountryDAO;
import com.javarush.domain.entity.City;
import com.javarush.domain.entity.Country;
import com.javarush.domain.entity.CountryLanguage;
import com.javarush.exception.DatabaseException;
import com.javarush.redis.CityCountry;
import com.javarush.redis.Language;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static SessionFactory sessionFactory;
    private static RedisClient redisClient;

    private static ObjectMapper mapper;

    private static CityDAO cityDAO;
    private static CountryDAO countryDAO;

    public static void main(String[] args) {
        sessionFactory = HibernateUtil.getSessionFactory();
        redisClient = RedisUtil.getClient();

        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);

        mapper = new ObjectMapper();

        List<City> allCities = fetchCitiesFromDb();
        List<CityCountry> preparedData = transformData(allCities);
        pushToRedis(preparedData);

        sessionFactory.getCurrentSession().close();

        List<Integer> ids = List.of(3, 240, 123, 4, 189, 89, 150, 118, 10, 220);

        long startRedis = System.currentTimeMillis();
        readFromRedis(ids);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        readFromSql(ids);
        long stopMysql = System.currentTimeMillis();

        long redisTime = stopRedis - startRedis;
        long mysqlTime = stopMysql - startMysql;

        logger.info("Redis time: {} ms", redisTime);
        logger.info("Mysql time: {} ms", mysqlTime);

        shutdown();
    }

    private static void pushToRedis(List<CityCountry> data) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (CityCountry cityCountry : data) {
                sync.set(String.valueOf(cityCountry.getId()), mapper.writeValueAsString(cityCountry));
            }
        }
        catch (JsonProcessingException e) {
            logger.error("Couldn't push CityCountry to Redis");
            throw new DatabaseException("Couldn't push CityCountry to Redis");
        }
    }

    private static void readFromRedis(List<Integer> ids) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (Integer id : ids) {
                String value = sync.get(String.valueOf(id));
                try {
                    mapper.readValue(value, CityCountry.class);
                } catch (JsonProcessingException e) {
                    logger.error("Couldn't parse CityCountry from Redis");
                    throw new DatabaseException("Couldn't parse CityCountry from Redis");
                }
            }
        }
    }

    private static void readFromSql(List<Integer> ids) {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            for (Integer id : ids) {
                City city = cityDAO.getById(id);
                Set<CountryLanguage> languages = city.getCountry().getLanguages();
            }
            session.getTransaction().commit();
        }
    }

    private static List<CityCountry> transformData(List<City> cities) {
        return cities.stream().map(city -> {
            CityCountry res = new CityCountry();
            res.setId(city.getId());
            res.setName(city.getName());
            res.setPopulation(city.getPopulation());
            res.setDistrict(city.getDistrict());

            Country country = city.getCountry();
            res.setAlternativeCountryCode(country.getAlternativeCode());
            res.setContinent(country.getContinent());
            res.setCountryCode(country.getCode());
            res.setCountryName(country.getName());
            res.setCountryPopulation(country.getPopulation());
            res.setCountryRegion(country.getRegion());
            res.setCountrySurfaceArea(country.getSurfaceArea());
            Set<CountryLanguage> countryLanguages = country.getLanguages();
            Set<Language> languages = countryLanguages.stream().map(cl -> {
                Language language = new Language();
                language.setLanguage(cl.getLanguage());
                language.setIsOfficial(cl.getIsOfficial());
                language.setPercentage(cl.getPercentage());
                return language;
            }).collect(Collectors.toSet());
            res.setLanguages(languages);

            return res;
        }).collect(Collectors.toList());
    }

    private static List<City> fetchCitiesFromDb() {
        try (Session session = sessionFactory.getCurrentSession()) {
            List<City> allCities = new ArrayList<>();
            session.beginTransaction();
            List<Country> countries = countryDAO.getAll();

            int totalCount = cityDAO.getTotalCount();
            int step = 500;
            for (int i = 0; i < totalCount; i += step) {
                allCities.addAll(cityDAO.getItems(i, step));
            }
            session.getTransaction().commit();
            return allCities;
        }
    }

    private static void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }
}