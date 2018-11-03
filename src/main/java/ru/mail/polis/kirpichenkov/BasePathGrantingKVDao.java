package ru.mail.polis.kirpichenkov;

import ru.mail.polis.KVDao;

import java.io.File;

public interface BasePathGrantingKVDao extends KVDao {

  File getBasePath();
}
