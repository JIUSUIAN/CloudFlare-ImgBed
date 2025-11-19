const tokenCache = new Map();

function encodeSegments(path) {
    if (!path) {
        return '';
    }
    return path
        .split('/')
        .filter(Boolean)
        .map(segment => encodeURIComponent(segment))
        .join('/');
}

function normalizeBaseFolder(folder) {
    if (!folder) {
        return '';
    }
    return folder.replace(/^\/+/, '').replace(/\/+$/, '');
}

export function buildOneDriveRemotePath(channelConfig = {}, fileId = '') {
    const baseFolder = normalizeBaseFolder(channelConfig.rootPath || channelConfig.baseFolder || '');
    const normalizedFileId = fileId.replace(/^\/+/, '');
    if (!baseFolder) {
        return normalizedFileId;
    }
    return `${baseFolder}/${normalizedFileId}`;
}

export class OneDriveClient {
    constructor(options = {}) {
        this.tenantId = options.tenantId;
        this.clientId = options.clientId;
        this.clientSecret = options.clientSecret;
        this.driveId = options.driveId;
        this.siteId = options.siteId;
        this.userPrincipalName = options.userPrincipalName;
    }

    get cacheKey() {
        return `${this.tenantId || ''}:${this.clientId || ''}`;
    }

    get baseResourcePath() {
        if (this.driveId) {
            return `/drives/${this.driveId}`;
        }
        if (this.userPrincipalName) {
            return `/users/${encodeURIComponent(this.userPrincipalName)}/drive`;
        }
        if (this.siteId) {
            return `/sites/${this.siteId}/drive`;
        }
        throw new Error('OneDrive channel is missing drive identifier (driveId/siteId/userPrincipalName)');
    }

    get baseUrl() {
        return `https://graph.microsoft.com/v1.0${this.baseResourcePath}`;
    }

    async getAccessToken() {
        if (!this.tenantId || !this.clientId || !this.clientSecret) {
            throw new Error('OneDrive channel credentials are incomplete');
        }

        const cached = tokenCache.get(this.cacheKey);
        const now = Date.now();
        if (cached && cached.expiresAt > now + 10000) {
            return cached.token;
        }

        const body = new URLSearchParams({
            client_id: this.clientId,
            client_secret: this.clientSecret,
            scope: 'https://graph.microsoft.com/.default',
            grant_type: 'client_credentials'
        });

        const tokenResponse = await fetch(`https://login.microsoftonline.com/${this.tenantId}/oauth2/v2.0/token`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body
        });

        if (!tokenResponse.ok) {
            const errorText = await tokenResponse.text();
            throw new Error(`Failed to acquire OneDrive token: ${errorText}`);
        }

        const tokenData = await tokenResponse.json();
        const expiresIn = tokenData.expires_in || 3600;
        tokenCache.set(this.cacheKey, {
            token: tokenData.access_token,
            expiresAt: now + expiresIn * 1000
        });

        return tokenData.access_token;
    }

    buildContentUrlFromPath(path) {
        const encodedPath = encodeSegments(path);
        return `${this.baseUrl}/root:/${encodedPath}:/content`;
    }

    buildItemUrl(itemId) {
        if (!itemId) {
            throw new Error('OneDrive itemId is required');
        }
        return `${this.baseUrl}/items/${itemId}`;
    }

    async simpleUpload(file, remotePath) {
        const token = await this.getAccessToken();
        const targetUrl = `${this.buildContentUrlFromPath(remotePath)}?@microsoft.graph.conflictBehavior=replace`;
        const response = await fetch(targetUrl, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': file.type || 'application/octet-stream'
            },
            body: file
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`OneDrive simple upload failed: ${errorText}`);
        }

        return await response.json();
    }

    async createUploadSession(remotePath, conflictBehavior = 'replace') {
        const token = await this.getAccessToken();
        const encodedPath = encodeSegments(remotePath);
        const targetUrl = `${this.baseUrl}/root:/${encodedPath}:/createUploadSession`;
        const pathSegments = encodedPath.split('/');
        const itemName = pathSegments[pathSegments.length - 1];

        const response = await fetch(targetUrl, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                item: {
                    '@microsoft.graph.conflictBehavior': conflictBehavior,
                    name: decodeURIComponent(itemName)
                }
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Failed to create OneDrive upload session: ${errorText}`);
        }

        return await response.json();
    }

    async uploadLargeFile(file, remotePath) {
        const session = await this.createUploadSession(remotePath);
        const chunkSize = 5 * 1024 * 1024; // 5MB per chunk
        let offset = 0;
        let lastResponse = null;

        while (offset < file.size) {
            const chunk = file.slice(offset, offset + chunkSize);
            const chunkBuffer = await chunk.arrayBuffer();
            const chunkLength = chunkBuffer.byteLength;
            const end = offset + chunkLength - 1;

            lastResponse = await this.uploadChunkToSession(session.uploadUrl, chunkBuffer, offset, end, file.size);
            if (lastResponse?.id) {
                return lastResponse;
            }

            offset += chunkLength;
        }

        return lastResponse;
    }

    async uploadChunkToSession(uploadUrl, chunkBuffer, start, end, totalSize) {
        const response = await fetch(uploadUrl, {
            method: 'PUT',
            headers: {
                'Content-Length': chunkBuffer.byteLength.toString(),
                'Content-Range': `bytes ${start}-${end}/${totalSize}`
            },
            body: chunkBuffer
        });

        if (response.status === 200 || response.status === 201) {
            return await response.json();
        }

        if (response.status === 202) {
            // 部分上传完成
            return null;
        }

        const errorText = await response.text();
        throw new Error(`OneDrive chunk upload failed: ${errorText}`);
    }

    async uploadFile(file, remotePath) {
        if (file.size <= 4 * 1024 * 1024) {
            return await this.simpleUpload(file, remotePath);
        }
        return await this.uploadLargeFile(file, remotePath);
    }

    async deleteItem(itemId, remotePath = '') {
        const token = await this.getAccessToken();
        let targetUrl = '';
        if (itemId) {
            targetUrl = this.buildItemUrl(itemId);
        } else if (remotePath) {
            const encodedPath = encodeSegments(remotePath);
            targetUrl = `${this.baseUrl}/root:/${encodedPath}`;
        } else {
            throw new Error('Either OneDrive itemId or path must be provided for deletion');
        }

        const response = await fetch(targetUrl, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (!response.ok && response.status !== 204) {
            const errorText = await response.text();
            throw new Error(`Failed to delete OneDrive file: ${errorText}`);
        }

        return true;
    }

    async moveItem(itemId, newFolderPath, newName) {
        if (!itemId) {
            throw new Error('OneDrive itemId is required for move operations');
        }
        const token = await this.getAccessToken();
        const targetUrl = this.buildItemUrl(itemId);
        const parentPath = newFolderPath
            ? `/drive/root:/${encodeSegments(newFolderPath)}`
            : '/drive/root:';

        const body = {
            parentReference: { path: parentPath }
        };
        if (newName) {
            body.name = newName;
        }

        const response = await fetch(targetUrl, {
            method: 'PATCH',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Failed to move OneDrive file: ${errorText}`);
        }

        return await response.json();
    }

    async getItemMetadata(itemId, remotePath = '') {
        const token = await this.getAccessToken();
        let targetUrl = '';
        if (itemId) {
            targetUrl = this.buildItemUrl(itemId);
        } else if (remotePath) {
            const encodedPath = encodeSegments(remotePath);
            targetUrl = `${this.baseUrl}/root:/${encodedPath}`;
        } else {
            throw new Error('Either OneDrive itemId or path must be provided');
        }

        const response = await fetch(targetUrl, {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Failed to fetch OneDrive metadata: ${errorText}`);
        }

        return await response.json();
    }

    async downloadContent(request, { itemId, remotePath }) {
        const token = await this.getAccessToken();
        const rangeHeader = request.headers.get('Range');
        const method = request.method === 'HEAD' ? 'GET' : request.method;
        let targetUrl = '';
        if (itemId) {
            targetUrl = `${this.buildItemUrl(itemId)}/content`;
        } else if (remotePath) {
            targetUrl = this.buildContentUrlFromPath(remotePath);
        } else {
            throw new Error('Either itemId or path must be provided to download content');
        }

        const headers = new Headers({ 'Authorization': `Bearer ${token}` });
        if (rangeHeader) {
            headers.set('Range', rangeHeader);
        }

        const response = await fetch(targetUrl, {
            method,
            headers
        });

        return response;
    }
}
