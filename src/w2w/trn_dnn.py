import logging, os
logging.basicConfig(level=logging.INFO)

from tqdm import tqdm

import torch
from torch import nn
import numpy as np
import torch.nn.functional as F
from torch.utils.data import DataLoader

from src.w2w.dataloader_dnn import w2w_dnn_loader

PARAMS = dict(
    SITE='Kabini',
    BATCH_SIZE=32,
    DATALOADER_WORKERS = 6,
    LR=0.0001,
    DEVICE='cuda',
    EPOCHS=20,
)


class W2W_DNN(nn.Module):
    def __init__(self, input_dim, output_dim):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, 120)
        self.fc2 = nn.Linear(120, 120)
        self.fc3 = nn.Linear(120, output_dim)

    def forward(self, x):
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        # activation?
        return x


def train(model, trn_loader, val_loader, optimizer, PARAMS, device, verbose=True):
    
    n_iter = 0
    for epoch in range(1, PARAMS['EPOCHS'] + 1):
        
        if verbose:
            pbar = tqdm(desc = f'EPOCH:{epoch}', total=len(trn_loader)+len(val_loader))
            
        trn_loss = 0
        
        for batch_idx, (X, Y) in enumerate(trn_loader):
            
            model.train()
            
            X, Y = X.to(device), Y.to(device)
            
            # zero the parameter gradients
            optimizer.zero_grad()

            # forward + backward + optimize
            Y_hat = model(X)
            
            loss = F.mse_loss(Y_hat, Y)
            loss.backward()
            optimizer.step()
        
            with torch.cuda.device(device):
                torch.cuda.empty_cache()
                
            trn_loss +=loss.detach().item()
                
            if verbose:
                pbar.update(PARAMS['BATCH_SIZE'])
                pbar.set_description(f'EPOCH:{epoch}, trn_loss:{trn_loss:.3f}')
                    
            del loss, X, Y
        
        model.eval()
        with torch.no_grad():
            val_loss = 0
            for batch_idx, (X, Y) in enumerate(val_loader):

                X, Y = X.to(device), Y.to(device)
                
                Y_hat = model(X)

                loss = F.mse_loss(Y_hat, Y)
                
                val_loss += loss.detach().item()
                
                if verbose:
                    pbar.update(PARAMS['BATCH_SIZE'])
                    pbar.set_description(f'EPOCH:{epoch}, trn_loss:{trn_loss:.3f}, val_loss:{val_loss:.3f}')
                
                del loss, X, Y
                
        if verbose:
            pbar.close()


    
def main():
    
    root = os.getcwd()
    
    device = torch.device(PARAMS['DEVICE'])
    
    trn_dataset = w2w_dnn_loader(
        csv_data_path=os.path.join(root, 'wave2web_data',f'{PARAMS["SITE"]}_12x3mo_split.csv'), 
        targets_forecast=60, # days 
        lag_windows=[5,15,30,60,100], # days
        segment='trn',
        normalise=True
    )
    val_dataset = w2w_dnn_loader(
        csv_data_path=os.path.join(root, 'wave2web_data',f'{PARAMS["SITE"]}_12x3mo_split.csv'), 
        targets_forecast=60, # days 
        lag_windows=[5,15,30,60,100], # days
        segment='val',
        normalise=True
    )
    
    trn_loader = DataLoader(
        trn_dataset, 
        batch_size=PARAMS['BATCH_SIZE'], 
        shuffle=False,
        num_workers=PARAMS['DATALOADER_WORKERS'], 
    )
    
    val_loader = DataLoader(
        val_dataset, 
        batch_size=PARAMS['BATCH_SIZE'], 
        shuffle=False,
        num_workers=PARAMS['DATALOADER_WORKERS'], 
    )
    
    _X, _Y = trn_dataset.__getitem__(0)
    
    model = W2W_DNN(
        input_dim  = _X.shape[0], 
        output_dim = _Y.shape[0]
    )
    
    model = model.to(device)
    
    optimizer = torch.optim.Adam(params=model.parameters(), lr=PARAMS['LR'])
    
    train(model, trn_loader, val_loader, optimizer, PARAMS, device=device, verbose=True)
    
    
if __name__=="__main__":
    main()
