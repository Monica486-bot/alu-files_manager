import { v4 as uuidv4 } from 'uuid';
import { ObjectId } from 'mongodb';
import fs from 'fs';
import path from 'path';
import mime from 'mime-types';
import Bull from 'bull';
import dbClient from '../utils/db';
import redisClient from '../utils/redis';

const fileQueue = new Bull('fileQueue');
const FOLDER_PATH = process.env.FOLDER_PATH || '/tmp/files_manager';

async function getUser(token) {
  if (!token) return null;
  const userId = await redisClient.get(`auth_${token}`);
  if (!userId) return null;
  return dbClient.db.collection('users').findOne({ _id: new ObjectId(userId) });
}

function formatFile(file) {
  const { _id, userId, name, type, isPublic, parentId, localPath } = file;
  const result = { id: _id, userId, name, type, isPublic, parentId };
  if (localPath) result.localPath = localPath;
  return result;
}

export default class FilesController {
  static async postUpload(req, res) {
    const user = await getUser(req.headers['x-token']);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });

    const { name, type, parentId = 0, isPublic = false, data } = req.body;
    if (!name) return res.status(400).json({ error: 'Missing name' });
    if (!type || !['folder', 'file', 'image'].includes(type)) return res.status(400).json({ error: 'Missing type' });
    if (!data && type !== 'folder') return res.status(400).json({ error: 'Missing data' });

    if (parentId !== 0) {
      const parent = await dbClient.db.collection('files').findOne({ _id: new ObjectId(parentId) });
      if (!parent) return res.status(400).json({ error: 'Parent not found' });
      if (parent.type !== 'folder') return res.status(400).json({ error: 'Parent is not a folder' });
    }

    const doc = { userId: user._id, name, type, isPublic, parentId };

    if (type === 'folder') {
      const result = await dbClient.db.collection('files').insertOne(doc);
      return res.status(201).json(formatFile({ ...doc, _id: result.insertedId }));
    }

    if (!fs.existsSync(FOLDER_PATH)) fs.mkdirSync(FOLDER_PATH, { recursive: true });
    const localPath = path.join(FOLDER_PATH, uuidv4());
    fs.writeFileSync(localPath, Buffer.from(data, 'base64'));

    doc.localPath = localPath;
    const result = await dbClient.db.collection('files').insertOne(doc);

    if (type === 'image') {
      fileQueue.add({ userId: user._id.toString(), fileId: result.insertedId.toString() });
    }

    return res.status(201).json(formatFile({ ...doc, _id: result.insertedId }));
  }

  static async getShow(req, res) {
    const user = await getUser(req.headers['x-token']);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });

    const file = await dbClient.db.collection('files').findOne({
      _id: new ObjectId(req.params.id),
      userId: user._id,
    });
    if (!file) return res.status(404).json({ error: 'Not found' });

    return res.status(200).json(formatFile(file));
  }

  static async getIndex(req, res) {
    const user = await getUser(req.headers['x-token']);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });

    const parentId = req.query.parentId || 0;
    const page = parseInt(req.query.page, 10) || 0;

    const files = await dbClient.db.collection('files').aggregate([
      { $match: { userId: user._id, parentId: parentId === 0 ? 0 : parentId } },
      { $skip: page * 20 },
      { $limit: 20 },
    ]).toArray();

    return res.status(200).json(files.map(formatFile));
  }

  static async putPublish(req, res) {
    const user = await getUser(req.headers['x-token']);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });

    const result = await dbClient.db.collection('files').findOneAndUpdate(
      { _id: new ObjectId(req.params.id), userId: user._id },
      { $set: { isPublic: true } },
      { returnDocument: 'after' },
    );
    if (!result.value) return res.status(404).json({ error: 'Not found' });

    return res.status(200).json(formatFile(result.value));
  }

  static async putUnpublish(req, res) {
    const user = await getUser(req.headers['x-token']);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });

    const result = await dbClient.db.collection('files').findOneAndUpdate(
      { _id: new ObjectId(req.params.id), userId: user._id },
      { $set: { isPublic: false } },
      { returnDocument: 'after' },
    );
    if (!result.value) return res.status(404).json({ error: 'Not found' });

    return res.status(200).json(formatFile(result.value));
  }

  static async getFile(req, res) {
    const file = await dbClient.db.collection('files').findOne({ _id: new ObjectId(req.params.id) });
    if (!file) return res.status(404).json({ error: 'Not found' });

    if (!file.isPublic) {
      const user = await getUser(req.headers['x-token']);
      if (!user || user._id.toString() !== file.userId.toString()) {
        return res.status(404).json({ error: 'Not found' });
      }
    }

    if (file.type === 'folder') return res.status(400).json({ error: "A folder doesn't have content" });

    const { size } = req.query;
    const filePath = size ? `${file.localPath}_${size}` : file.localPath;

    if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'Not found' });

    return res.status(200).type(mime.lookup(file.name) || 'application/octet-stream').sendFile(filePath);
  }
}
