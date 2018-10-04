/**
 * Copyright 2018 Marcus Klang (marcus.klang@cs.lth.se)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package se.lth.cs.docria.io;

import java.io.IOException;
import java.io.OutputStream;

public class DocumentStreamBoundaryWriter extends DocumentStreamWriter {
    private BoundaryWriter writer;

    public DocumentStreamBoundaryWriter(OutputStream outputStream) throws IOException {
        super(new BoundaryWriter(outputStream));
    }

    public DocumentStreamBoundaryWriter(OutputStream outputStream, int numDocsPerBlock) throws IOException {
        super(new BoundaryWriter(outputStream), numDocsPerBlock);
    }

    public DocumentStreamBoundaryWriter(OutputStream outputStream, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream), codec);
    }

    public DocumentStreamBoundaryWriter(OutputStream outputStream, int numDocsPerBlock, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream), numDocsPerBlock, codec);
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream) throws IOException {
        super(new BoundaryWriter(outputStream, boundary));
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream, int numDocsPerBlock) throws IOException {
        super(new BoundaryWriter(outputStream, boundary), numDocsPerBlock);
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream, boundary), codec);
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream, int numDocsPerBlock, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream, boundary), numDocsPerBlock, codec);
    }

    @Override
    protected void flushblock() {
        ((BoundaryWriter)outputStream).split();
        super.flushblock();
    }

}
