/**
 * @license
 * Copyright 2022 Marco Fucci di Napoli (mfucci@gmail.com)
 * SPDX-License-Identifier: Apache-2.0
 */

import { Singleton } from "../util/Singleton";
import { Fabric } from "./Fabric";

export const getFabricManager = Singleton(() => new FabricManager());

export class FabricManager {
    private readonly fabrics = new Array<Fabric>();

    addFabric(fabric: Fabric) {
        this.fabrics.push(fabric);
    }

    getFabrics() {
        return this.fabrics;
    }

    findFabricFromDestinationId(destinationId: Buffer, initiatorRandom: Buffer) {
        console.log("destinationId", destinationId.toString("hex"));
        console.log("initiatorRandom", initiatorRandom.toString("hex"));

        for (var fabric of this.fabrics) {
            const candidateDestinationId = fabric.getDestinationId(initiatorRandom);
            console.log("candidate", candidateDestinationId.toString("hex"));
            if (!candidateDestinationId.equals(destinationId)) continue;
            return fabric;
        }
        
        throw new Error("Fabric cannot be found from destinationId");
    }
}