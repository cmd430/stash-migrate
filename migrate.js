import { randomUUID } from 'node:crypto'
import { resolve, extname } from 'node:path'
import { Readable } from 'node:stream'
import { createReadStream } from 'node:fs'
import { glob } from 'glob'
import Database from 'better-sqlite3'
import { Log } from 'cmd430-utils'
import { config } from '../config/config.js'
import { getDatabaseInterface } from '../interfaces/database.js'
import { getStorageInterface } from '../interfaces/storage.js'
import generateThumbnail from '../utils/generateThumbnail.js'
import { getMimeExtension } from '../utils/mimetype.js'

// eslint-disable-next-line no-unused-vars
const { log, debug, info, warn, error } = new Log('Migrate')
const getExtname = (fn, mt) => extname(fn).length > 0 ? '' : getMimeExtension(mt)
const DataStore = await getDatabaseInterface(config.database.store)
const FileStore = await getStorageInterface(config.storage.store)
const fastify = {
  storage: new FileStore(),
  db: new DataStore()
}

await fastify.db.connect()

const oldDB = new Database('./storage/database/stash.db', {
  readonly: false,
  fileMustExist: false,
  timeout: 5000,
  verbose: null
})

info('Starting Migration')

const users = `
  SELECT NULL AS "id",
    "username",
    "email",
    "password",
    "admin" AS "isAdmin"
  FROM
    "users"
`
for (const { username, email, password, isAdmin } of oldDB.prepare(users).all()) {
  await fastify.db.createAccount({
    id: randomUUID(),
    username: username,
    email: email,
    password: password,
    isAdmin: isAdmin
  })
  await fastify.storage.createContainer(username)
}

const files = `
  SELECT
    "file_id" AS "id",
    "original_filename" AS "name",
    '*/'||rtrim("mimetype", replace("mimetype", '/', '' ) )||"file_id"||'*' AS "file",
    "filesize" AS "size",
    "mimetype" AS "type",
    "uploaded_by" AS "uploadedBy",
    "uploaded_at" AS "uploadedAt",
    ifnull("uploaded_until", 'Infinity') AS "uploadedUntil",
    NOT "public" AS "isPrivate",
    "in_album" AS "inAlbum",
    NULL AS "albumOrder"
  FROM
    "files"
  ORDER BY
    "in_album"
`
const addedAlbums = []
for (const file of oldDB.prepare(files).all()) {
  const { id, file: oldFile, type: mimetype, name, uploadedBy, uploadedAt, uploadedUntil, isPrivate, inAlbum } = file
  const [ filepath ] = await glob(oldFile)

  if (!filepath) continue

  info('Processing file', id)

  const filename = `${name}${getExtname(name, mimetype)}`
  const { filename: storageFilename, thumbnailFilename: storageThumbnailFilename } = fastify.storage.create(uploadedBy, filename)

  const { filesize } = await fastify.storage.write({
    username: uploadedBy,
    file: {
      filename: storageFilename,
      filestream: createReadStream(resolve(filepath))
    },
    thumbnail: {
      filename: storageThumbnailFilename,
      filestream: createReadStream(resolve('..', 'thumbnail', `${id}.webp`))
    }
  })

  if (inAlbum && addedAlbums.includes(inAlbum)) {
    await fastify.db.addFile({
      album: inAlbum,
      id: id,
      name: filename,
      file: storageFilename,
      size: filesize,
      type: mimetype,
      uploadedBy: uploadedBy
    })
  } else {
    await fastify.db.addFile({
      id: id,
      name: filename,
      file: storageFilename,
      size: filesize,
      type: mimetype,
      uploadedBy: uploadedBy,
      uploadedAt: uploadedAt,
      uploadedUntil: uploadedUntil,
      isPrivate: isPrivate
    })

    if (inAlbum && !addedAlbums.includes(inAlbum)) {
      await fastify.db.createAlbum({
        id: inAlbum,
        files: [ id ],
        uploadedBy: uploadedBy,
        uploadedAt: uploadedAt,
        uploadedUntil: uploadedUntil,
        isPrivate: isPrivate
      })
      addedAlbums.push(inAlbum)
    }
  }
}

info('Migration Complete')
