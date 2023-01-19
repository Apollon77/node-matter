/**
 * @license
 * Copyright 2022 The node-matter Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { MatterDevice } from "../MatterDevice";
import { ProtocolHandler } from "../common/ProtocolHandler";
import { MessageExchange } from "../common/MessageExchange";
import { InteractionServerMessenger, InvokeRequest, InvokeResponse, ReadRequest, DataReport, SubscribeRequest, SubscribeResponse, TimedRequest } from "./InteractionMessenger";
import { CommandServer, ResultCode } from "../cluster/server/CommandServer";
import { DescriptorCluster } from "../cluster/DescriptorCluster";
import { AttributeGetterServer, AttributeServer } from "../cluster/server/AttributeServer";
import { Cluster } from "../cluster/Cluster";
import { AttributeServers, AttributeInitialValues, ClusterServerHandlers } from "../cluster/server/ClusterServer";
import { SecureSession } from "../session/SecureSession";
import { SubscriptionHandler } from "./SubscriptionHandler";
import { Logger } from "../../log/Logger";
import { TlvStream, TypeFromBitSchema } from "@project-chip/matter.js";
import { Endpoint } from "./Endpoint";
import { Device, DEVICE } from "../common/DeviceTypes";
import { capitalize } from "../../util/String";
import { AtLeastOne } from "../../util/Array";
import { StatusCode } from "./InteractionMessages";

export const INTERACTION_PROTOCOL_ID = 0x0001;

export class ClusterServer<ClusterT extends Cluster<any, any, any, any>> {
    readonly id: number;
    readonly name: string;
    readonly attributes = <AttributeServers<ClusterT["attributes"]>>{};
    readonly commands = new Array<CommandServer<any, any>>();

    constructor(clusterDef: ClusterT, features: TypeFromBitSchema<ClusterT["features"]>, attributesInitialValues: AttributeInitialValues<ClusterT["attributes"]>, handlers: ClusterServerHandlers<ClusterT>) {
        const { id, name, attributes: attributeDefs, commands: commandDefs } = clusterDef;
        this.id = id;
        this.name = name;

        // Create attributes
        attributesInitialValues = {
            ...attributesInitialValues,
            clusterRevision: clusterDef.revision,
            featureMap: features,
        };
        for (const name in attributesInitialValues) {
            const { id, schema, validator } = attributeDefs[name];
            const getter = (handlers as any)[`get${capitalize(name)}`];
            if (getter === undefined) {
                (this.attributes as any)[name] = new AttributeServer(id, name, schema, validator ?? (() => {}), (attributesInitialValues as any)[name]);
            } else {
                (this.attributes as any)[name] = new AttributeGetterServer(id, name, schema, validator ?? (() => {}), (attributesInitialValues as any)[name], getter);
            }
        }

        // Create commands
        for (const name in commandDefs) {
            const handler = (handlers as any)[name];
            if (handler === undefined) continue;
            const { requestId, requestSchema, responseId, responseSchema } = commandDefs[name];
            this.commands.push(new CommandServer(requestId, responseId, name, requestSchema, responseSchema, (request, session) => handler({request, attributes: this.attributes, session})));
        }
    }
}

export interface Path {
    endpointId: number,
    clusterId: number,
    id: number,
}

export interface AttributeWithPath {
    path: Path,
    attribute: AttributeServer<any>,
}

export function pathToId({endpointId, clusterId, id}: Path) {
    return `${endpointId}/${clusterId}/${id}`;
}

function toHex(value: number | undefined) {
    return value === undefined ? "*" : `0x${value.toString(16)}`;
}

const logger = Logger.get("InteractionProtocol");

export class InteractionServer implements ProtocolHandler<MatterDevice> {
    private endpoints = new Map<number, { deviceTypes: AtLeastOne<Device>, clusters: Map<number, ClusterServer<any>> }>();
    private attributes = new Map<string, AttributeServer<any>>();
    private attributePaths = new Array<Path>();
    private commands = new Map<string, CommandServer<any, any>>();
    private commandPaths = new Array<Path>();

    private rootEndpoint = new Endpoint(0, [ DEVICE.ROOT ], [], []);

    constructor() {}

    getId() {
        return INTERACTION_PROTOCOL_ID;
    }

    addRootEndpoint(clusters: ClusterServer<any>[], childrenEndpoints?: Endpoint[]) {
        this.rootEndpoint = new Endpoint(0, [ DEVICE.ROOT ], clusters, childrenEndpoints);
        this.endpoints = this.rootEndpoint.endpoints;
        this.attributes = this.rootEndpoint.attributes;
        this.attributePaths = this.rootEndpoint.attributePaths;
        this.commands = this.rootEndpoint.commands;
        this.commandPaths = this.rootEndpoint.commandPaths;

        return this;
    }

    addEndpoint(endpointId: number, deviceTypes: AtLeastOne<Device>, clusters: ClusterServer<any>[]) {
        this.rootEndpoint.addEndpoint(endpointId, deviceTypes, clusters);

        return this;
    }

    async onNewExchange(exchange: MessageExchange<MatterDevice>) {
        await new InteractionServerMessenger(exchange).handleRequest(
            readRequest => this.handleReadRequest(exchange, readRequest),
            subscribeRequest => this.handleSubscribeRequest(exchange, subscribeRequest),
            invokeRequest => this.handleInvokeRequest(exchange, invokeRequest),
            timedRequest => this.handleTimedRequest(exchange, timedRequest),
        );
    }

    handleReadRequest(exchange: MessageExchange<MatterDevice>, {attributes: attributePaths}: ReadRequest): DataReport {
        logger.debug(`Received read request from ${exchange.channel.getName()}: ${attributePaths.map(path => this.resolveAttributeName(path)).join(", ")}`);

        const values = this.getAttributes(attributePaths)
            .map(({ path, attribute }) => {
                const { value, version } = attribute.getWithVersion(exchange.session);
                return { path, value, version, schema: attribute.schema };
            });

        return {
            interactionModelRevision: 1,
            values: values.map(({ path, value, version, schema }) => ({
                value: {
                    path,
                    version,
                    value: schema.encodeTlv(value),
                },
            })),
        };
    }

    handleSubscribeRequest(exchange: MessageExchange<MatterDevice>, { minIntervalFloorSeconds, maxIntervalCeilingSeconds, attributeRequests, keepSubscriptions }: SubscribeRequest): SubscribeResponse | undefined {
        logger.debug(`Received subscribe request from ${exchange.channel.getName()}`);

        if (!exchange.session.isSecure()) throw new Error("Subscriptions are only implemented on secure sessions");
        const session = exchange.session as SecureSession<MatterDevice>;
        const fabric = session.getFabric();
        if (fabric === undefined) throw new Error("Subscriptions are only implemented after a fabric has been assigned");

        if (!keepSubscriptions) {
            session.clearSubscriptions();
        }

        if (attributeRequests !== undefined) {
            logger.debug(`Subscribe to ${attributeRequests.map(path => this.resolveAttributeName(path)).join(", ")}`);
            let attributes = this.getAttributes(attributeRequests);

            if (attributeRequests.length === 0) throw new Error("Invalid subscription request");
            if (minIntervalFloorSeconds < 0) throw new Error("minIntervalFloorSeconds should be greater or equal to 0");
            if (maxIntervalCeilingSeconds < 0) throw new Error("maxIntervalCeilingSeconds should be greater or equal to 1");
            if (maxIntervalCeilingSeconds < minIntervalFloorSeconds) throw new Error("maxIntervalCeilingSeconds should be greater or equal to minIntervalFloorSeconds");

            const subscriptionId = session.addSubscription(SubscriptionHandler.Builder(session.getContext(), fabric, session.getPeerNodeId(), attributes, minIntervalFloorSeconds, maxIntervalCeilingSeconds));

            return { subscriptionId, maxIntervalCeilingSeconds, interactionModelRevision: 1 };
        }
    }

    async handleInvokeRequest(exchange: MessageExchange<MatterDevice>, {invokes}: InvokeRequest): Promise<InvokeResponse> {
        logger.debug(`Received invoke request from ${exchange.channel.getName()}: ${invokes.map(({path: {endpointId, clusterId, id}}) => `${toHex(endpointId)}/${toHex(clusterId)}/${toHex(id)}`).join(", ")}`);

        const results = new Array<{path: Path, code: ResultCode, response: TlvStream, responseId: number }>();

        await Promise.all(invokes.map(async ({ path, args }) => {
            const command = this.commands.get(pathToId(path));
            if (command === undefined) return;
            const result = await command.invoke(exchange.session, args);
            results.push({ ...result, path });
        }));

        return {
            suppressResponse: false,
            interactionModelRevision: 1,
            responses: results.map(({path, responseId, code, response}) => {
                if (response.length === 0) {
                    return { result: { path, result: { code }} };
                } else {
                    return { response: { path: { ...path, id: responseId }, response} };
                }
            }),
        };
    }

    async handleTimedRequest(exchange: MessageExchange<MatterDevice>, {timeout}: TimedRequest) {
        logger.debug(`Received timed request from ${exchange.channel.getName()}`);
        // TODO: implement this
    }

    private resolveAttributeName({ endpointId, clusterId, id }: Partial<Path>) {
        if (endpointId === undefined) {
            return `*/${toHex(clusterId)}/${toHex(id)}`;
        }
        const endpoint = this.endpoints.get(endpointId);
        if (endpoint === undefined) {
            return `unknown(${toHex(endpointId)})/${toHex(clusterId)}/${toHex(id)}`;
        }
        const endpointName = `${endpoint.deviceTypes[0].name}(${toHex(endpointId)})`;

        if (clusterId === undefined) {
            return `${endpointName}/*/${toHex(id)}`;
        }
        const cluster = endpoint.clusters.get(clusterId);
        if (cluster === undefined) {
            return `${endpointName}/unknown(${toHex(clusterId)})/${toHex(id)}`;
        }
        const clusterName = `${cluster.name}(${toHex(clusterId)})`;

        if (id === undefined) {
            return `${endpointName}/${clusterName}/*`;
        }
        const attribute = this.attributes.get(pathToId({ endpointId, clusterId, id }));
        const attributeName = `${attribute?.name ?? "unknown"}(${toHex(id)})`;
        return `${endpointName}/${clusterName}/${attributeName}`;
    }

    private getAttributes(filters: Partial<Path>[] ): AttributeWithPath[] {
        const result = new Array<AttributeWithPath>();

        filters.forEach(({ endpointId, clusterId, id }) => {
            if (endpointId !== undefined && clusterId !== undefined && id !== undefined) {
                const path = { endpointId, clusterId, id };
                const attribute = this.attributes.get(pathToId(path));
                if (attribute === undefined) return;
                result.push({ path, attribute });
            } else {
                this.attributePaths.filter(path =>
                    (endpointId === undefined || endpointId === path.endpointId)
                    && (clusterId === undefined || clusterId === path.clusterId)
                    && (id === undefined || id === path.id))
                    .forEach(path => result.push({ path, attribute: this.attributes.get(pathToId(path)) as AttributeServer<any> }));
            }
        })

        return result;
    }
}
