import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import os
from datetime import datetime
import csv

# Set random seed for reproducibility
torch.manual_seed(42)
np.random.seed(42)

# Convert lists to numpy arrays - stack the list directly
train_data_array = np.vstack(train_data)
test_data_array = np.vstack(test_data)

train_labels_array = np.array(train_labels, dtype=np.float32).reshape(-1, 1)
test_labels_array = np.array(test_labels, dtype=np.float32).reshape(-1, 1)

print(f"Original data shape: {train_data_array.shape}")
print(f"Original labels shape: {train_labels_array.shape}")

# ============================================================================
# UNIFORMLY SHIFT LABELS TO BE POSITIVE
# ============================================================================
print("\nShifting labels to make all positive...")

# Find the minimum value across ALL labels (train + test)
all_labels = np.concatenate([train_labels_array, test_labels_array])
min_label = np.min(all_labels)
max_label = np.max(all_labels)

print(f"Original label range: [{min_label:.6f}, {max_label:.6f}]")

# Calculate shift amount needed
if min_label < 0:
    shift_amount = abs(min_label) + 0.001
else:
    shift_amount = 0.001 - min_label

# Apply uniform shift to all labels
train_labels_array = train_labels_array + shift_amount
test_labels_array = test_labels_array + shift_amount

# Verify new range
new_min = np.min(np.concatenate([train_labels_array, test_labels_array]))
new_max = np.max(np.concatenate([train_labels_array, test_labels_array]))

print(f"Shift amount applied: {shift_amount:.6f}")
print(f"New label range: [{new_min:.6f}, {new_max:.6f}]")
print(f"All labels are now positive: {new_min > 0}")

# Save shift amount for reversing predictions later
label_shift = shift_amount
# ============================================================================

print(f"Labels dtype: {train_labels_array.dtype}")

# Split training data: 90% for training, 10% for validation
X_train, X_val, y_train, y_val = train_test_split(
    train_data_array, train_labels_array, test_size=0.1, random_state=42
)

# NO STANDARDIZATION - use raw features
X_train_scaled = X_train
X_val_scaled = X_val
X_test_scaled = test_data_array

# Convert to PyTorch tensors
X_train_tensor = torch.FloatTensor(X_train_scaled)
y_train_tensor = torch.FloatTensor(y_train)
X_val_tensor = torch.FloatTensor(X_val_scaled)
y_val_tensor = torch.FloatTensor(y_val)
X_test_tensor = torch.FloatTensor(X_test_scaled)
y_test_tensor = torch.FloatTensor(test_labels_array)

# Create DataLoaders
batch_size = 64
train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

val_dataset = TensorDataset(X_val_tensor, y_val_tensor)
val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)

test_dataset = TensorDataset(X_test_tensor, y_test_tensor)
test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)


# Define the neural network architecture
class RegressionNet(nn.Module):
    def __init__(self, input_size=405, hidden_sizes=[128, 64], dropout_rate=0.5, use_batchnorm=True):
        super(RegressionNet, self).__init__()

        layers = []
        prev_size = input_size

        for hidden_size in hidden_sizes:
            layers.append(nn.Linear(prev_size, hidden_size))
            if use_batchnorm:
                layers.append(nn.BatchNorm1d(hidden_size))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout_rate))
            prev_size = hidden_size

        # Output layer
        layers.append(nn.Linear(prev_size, 1))

        self.network = nn.Sequential(*layers)

    def forward(self, x):
        return self.network(x)


# Hyperparameter search space
hyperparam_grid = {
    'learning_rate': [0.0001, 0.00005, 0.0002],
    'hidden_sizes': [[128, 64], [96, 48], [64, 32]],
    'dropout_rate': [0.4, 0.5, 0.6],
    'weight_decay': [0.01, 0.005, 0.02]
}

# Initial hyperparameters
current_lr = 0.0001
current_hidden = [128, 64]
current_dropout = 0.5
current_weight_decay = 0.01
use_batchnorm = True


# Training function
def train_epoch(model, loader, criterion, optimizer, device):
    model.train()
    total_loss = 0
    for X_batch, y_batch in loader:
        X_batch, y_batch = X_batch.to(device), y_batch.to(device)

        optimizer.zero_grad()
        predictions = model(X_batch)
        loss = criterion(predictions, y_batch)
        loss.backward()
        optimizer.step()

        total_loss += loss.item()

    return total_loss / len(loader)


# Evaluation function
def evaluate(model, loader, criterion, device):
    model.eval()
    total_loss = 0
    with torch.no_grad():
        for X_batch, y_batch in loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            predictions = model(X_batch)
            loss = criterion(predictions, y_batch)
            total_loss += loss.item()

    return total_loss / len(loader)


# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"\nUsing device: {device}")

# Verify CUDA is available and working
if torch.cuda.is_available():
    print(f"CUDA Device: {torch.cuda.get_device_name(0)}")
    print(f"CUDA Memory Available: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
else:
    print("WARNING: CUDA not available, using CPU")
print(f"Training samples: {len(X_train)}")
print(f"Validation samples: {len(X_val)}")
print(f"Test samples: {len(test_data_array)}")

# Create checkpoint directory with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
model_type = 'rap_box'
checkpoint_dir = f"model_{model_type}_{timestamp}"
os.makedirs(checkpoint_dir, exist_ok=True)
print(f"Checkpoint directory: {checkpoint_dir}\n")

# Initialize model
print(f"Initial model configuration:")
print(f"  Input size: 405")
print(f"  Hidden layers: {current_hidden}")
print(f"  Dropout rate: {current_dropout}")
print(f"  Learning rate: {current_lr}")
print(f"  Weight decay: {current_weight_decay}")
print(f"  Batch normalization: {use_batchnorm}")
print(f"  Feature standardization: DISABLED")

model = RegressionNet(input_size=405, hidden_sizes=current_hidden,
                      dropout_rate=current_dropout, use_batchnorm=use_batchnorm).to(device)
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=current_lr, weight_decay=current_weight_decay)

# Training loop with hyperparameter tuning every 20 epochs
num_epochs = 100
best_val_loss = float('inf')
best_epoch = 0
best_model_state = None
best_hyperparams = None
patience = 20
epochs_without_improvement = 0
hyperparam_tune_interval = 20  # Tune hyperparameters every 20 epochs

# Track history
train_losses = []
val_losses = []
test_losses = []

print("\nStarting training with periodic hyperparameter tuning...")
for epoch in range(num_epochs):
    # Hyperparameter tuning every N epochs
    if epoch > 0 and epoch % hyperparam_tune_interval == 0:
        print(f"\n{'=' * 60}")
        print(f"Running hyperparameter search at epoch {epoch}...")
        print(f"{'=' * 60}")

        best_combo_val_loss = best_val_loss
        best_combo_lr = current_lr
        best_combo_hidden = current_hidden
        best_combo_dropout = current_dropout
        best_combo_wd = current_weight_decay

        # Test different hyperparameter combinations
        for lr in hyperparam_grid['learning_rate']:
            for hidden in hyperparam_grid['hidden_sizes']:
                for dropout in hyperparam_grid['dropout_rate']:
                    for wd in hyperparam_grid['weight_decay']:
                        # Create temporary model
                        temp_model = RegressionNet(input_size=405, hidden_sizes=hidden,
                                                   dropout_rate=dropout, use_batchnorm=use_batchnorm).to(device)
                        temp_optimizer = optim.Adam(temp_model.parameters(), lr=lr, weight_decay=wd)

                        # Train for 3 epochs to evaluate
                        for _ in range(3):
                            train_epoch(temp_model, train_loader, criterion, temp_optimizer, device)

                        temp_val_loss = evaluate(temp_model, val_loader, criterion, device)

                        if temp_val_loss < best_combo_val_loss:
                            best_combo_val_loss = temp_val_loss
                            best_combo_lr = lr
                            best_combo_hidden = hidden
                            best_combo_dropout = dropout
                            best_combo_wd = wd

        # Update hyperparameters if better combination found
        if (best_combo_lr != current_lr or best_combo_hidden != current_hidden or
            best_combo_dropout != current_dropout or best_combo_wd != current_weight_decay):
            print(f"Updating hyperparameters:")
            print(f"  LR: {current_lr} → {best_combo_lr}")
            print(f"  Hidden: {current_hidden} → {best_combo_hidden}")
            print(f"  Dropout: {current_dropout} → {best_combo_dropout}")
            print(f"  Weight Decay: {current_weight_decay} → {best_combo_wd}")

            current_lr = best_combo_lr
            current_hidden = best_combo_hidden
            current_dropout = best_combo_dropout
            current_weight_decay = best_combo_wd

            # Reinitialize model with new hyperparameters
            model = RegressionNet(input_size=405, hidden_sizes=current_hidden,
                                  dropout_rate=current_dropout, use_batchnorm=use_batchnorm).to(device)
            optimizer = optim.Adam(model.parameters(), lr=current_lr, weight_decay=current_weight_decay)
        else:
            print(f"Keeping current hyperparameters (no improvement found)")
        print(f"{'=' * 60}\n")

    # Train for one epoch
    train_loss = train_epoch(model, train_loader, criterion, optimizer, device)
    val_loss = evaluate(model, val_loader, criterion, device)
    test_loss = evaluate(model, test_loader, criterion, device)

    train_losses.append(train_loss)
    val_losses.append(val_loss)
    test_losses.append(test_loss)

    print(
        f"Epoch [{epoch + 1}/{num_epochs}] - Train Loss: {train_loss:.6f}, Val Loss: {val_loss:.6f}, Test Loss: {test_loss:.6f}")

    # Update best model if validation loss improved
    if val_loss < best_val_loss:
        best_val_loss = val_loss
        best_epoch = epoch + 1
        best_model_state = model.state_dict().copy()
        best_hyperparams = {
            'learning_rate': current_lr,
            'hidden_sizes': current_hidden,
            'dropout_rate': current_dropout,
            'weight_decay': current_weight_decay,
            'use_batchnorm': use_batchnorm
        }
        epochs_without_improvement = 0

        # Save checkpoint
        checkpoint_path = os.path.join(checkpoint_dir, 'best_model.pth')
        torch.save({
            'epoch': best_epoch,
            'model_state_dict': best_model_state,
            'train_loss': train_loss,
            'val_loss': val_loss,
            'test_loss': test_loss,
            'label_shift': label_shift,
            'hyperparameters': best_hyperparams
        }, checkpoint_path)

        # Save human-readable summary
        summary_path = os.path.join(checkpoint_dir, 'best_model_info.txt')
        with open(summary_path, 'w') as f:
            f.write(f"Best Model Information\n")
            f.write(f"{'=' * 60}\n")
            f.write(f"Saved at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Epoch: {best_epoch}\n")
            f.write(f"Training Loss: {train_loss:.6f}\n")
            f.write(f"Validation Loss: {val_loss:.6f}\n")
            f.write(f"Test Loss: {test_loss:.6f}\n")
            f.write(f"\nLabel Transformation:\n")
            f.write(f"  Uniform Shift: +{label_shift:.6f}\n")
            f.write(f"  To restore original scale: prediction - {label_shift:.6f}\n")
            f.write(f"\nModel Configuration:\n")
            f.write(f"  Learning Rate: {current_lr}\n")
            f.write(f"  Hidden Layers: {current_hidden}\n")
            f.write(f"  Dropout Rate: {current_dropout}\n")
            f.write(f"  Weight Decay: {current_weight_decay}\n")
            f.write(f"  Batch Normalization: {use_batchnorm}\n")
            f.write(f"  Feature Standardization: DISABLED\n")
            f.write(f"{'=' * 60}\n")

        print(f"  *** New best model saved! (Val Loss: {val_loss:.6f}) ***")
    else:
        epochs_without_improvement += 1
        print(f"  No improvement for {epochs_without_improvement} epochs")

        if epochs_without_improvement >= patience:
            print(f"\nEarly stopping triggered! No improvement for {patience} epochs.")
            print(f"Best validation loss: {best_val_loss:.6f} at epoch {best_epoch}")
            break

# Load best model
model.load_state_dict(best_model_state)

# Final evaluation on test set
test_loss = evaluate(model, test_loader, criterion, device)

# Get predictions for all test samples
model.eval()
all_predictions = []
all_actual = []

with torch.no_grad():
    for X_batch, y_batch in test_loader:
        X_batch = X_batch.to(device)
        predictions = model(X_batch)
        all_predictions.extend(predictions.cpu().numpy().flatten())
        all_actual.extend(y_batch.numpy().flatten())

# Convert to numpy arrays
all_predictions = np.array(all_predictions)
all_actual = np.array(all_actual)

print(f"\n{'=' * 60}")
print(f"Training complete!")
print(f"Best epoch: {best_epoch}")
print(f"Best validation loss: {best_val_loss:.6f}")
print(f"Final test loss: {test_loss:.6f}")
print(f"Best hyperparameters: {best_hyperparams}")
print(f"Label shift applied: +{label_shift:.6f}")
print(f"{'=' * 60}")

# Print test predictions vs actual
print(f"\n{'=' * 60}")
print(f"Test Set Predictions (First 20 samples)")
print(f"{'=' * 60}")
print(f"{'Index':<8} {'Predicted':<15} {'Actual':<15} {'Error':<15}")
print(f"{'-' * 60}")
for i in range(min(20, len(all_predictions))):
    error = all_predictions[i] - all_actual[i]
    print(f"{i:<8} {all_predictions[i]:<15.6f} {all_actual[i]:<15.6f} {error:<15.6f}")
if len(all_predictions) > 20:
    print(f"... ({len(all_predictions) - 20} more samples)")

# Calculate and print error statistics
mae = np.mean(np.abs(all_predictions - all_actual))
rmse = np.sqrt(np.mean((all_predictions - all_actual) ** 2))
print(f"\n{'=' * 60}")
print(f"Test Set Error Statistics:")
print(f"  Mean Absolute Error (MAE): {mae:.6f}")
print(f"  Root Mean Squared Error (RMSE): {rmse:.6f}")
print(f"{'=' * 60}")

# Save final training summary
final_summary_path = os.path.join(checkpoint_dir, 'training_summary.txt')
with open(final_summary_path, 'w') as f:
    f.write(f"Training Summary\n")
    f.write(f"{'=' * 60}\n")
    f.write(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Total Epochs: {len(train_losses)}\n")
    f.write(f"Best Epoch: {best_epoch}\n")
    f.write(f"\nLabel Transformation:\n")
    f.write(f"  Uniform Shift Applied: +{label_shift:.6f}\n")
    f.write(f"  All labels shifted to positive range\n")
    f.write(f"  To restore original predictions: subtract {label_shift:.6f}\n")
    f.write(f"\nDataset Statistics:\n")
    f.write(f"  Training Samples: {len(X_train)}\n")
    f.write(f"  Validation Samples: {len(X_val)}\n")
    f.write(f"  Test Samples: {len(test_data_array)}\n")
    f.write(f"  Number of Features: {X_train.shape[1]}\n")
    f.write(f"\nBest Model Performance:\n")
    f.write(f"  Validation Loss: {best_val_loss:.6f}\n")
    f.write(f"  Final Test Loss: {test_loss:.6f}\n")
    f.write(f"  Mean Absolute Error (MAE): {mae:.6f}\n")
    f.write(f"  Root Mean Squared Error (RMSE): {rmse:.6f}\n")
    f.write(f"\nBest Hyperparameters:\n")
    for key, value in best_hyperparams.items():
        f.write(f"  {key}: {value}\n")
    f.write(f"{'=' * 60}\n")

# Save test predictions to CSV
predictions_csv_path = os.path.join(checkpoint_dir, 'test_predictions.csv')
with open(predictions_csv_path, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Index', 'Predicted_Shifted', 'Actual_Shifted', 'Error_Shifted',
                     'Predicted_Original', 'Actual_Original', 'Error_Original'])
    for i in range(len(all_predictions)):
        error_shifted = all_predictions[i] - all_actual[i]
        pred_orig = all_predictions[i] - label_shift
        actual_orig = all_actual[i] - label_shift
        error_orig = pred_orig - actual_orig
        writer.writerow([i, all_predictions[i], all_actual[i], error_shifted,
                         pred_orig, actual_orig, error_orig])
print(f"Test predictions saved to: {predictions_csv_path}")

print(f"\nAll results saved in: {checkpoint_dir}")

# Plot training history
plt.figure(figsize=(10, 6))
plt.plot(train_losses, label='Training Loss')
plt.plot(val_losses, label='Validation Loss')
plt.plot(test_losses, label='Test Loss')
plt.xlabel('Epoch')
plt.ylabel('Loss (MSE)')
plt.title('Training, Validation, and Test Loss Over Time')
plt.legend()
plt.grid(True)
plot_path = os.path.join(checkpoint_dir, 'training_history.png')
plt.savefig(plot_path, dpi=300, bbox_inches='tight')
plt.show()
print(f"Training plot saved to: {plot_path}")

# Save loss history as CSV
csv_path = os.path.join(checkpoint_dir, 'loss_history.csv')
with open(csv_path, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Epoch', 'Train_Loss', 'Val_Loss', 'Test_Loss'])
    for i, (train_l, val_l, test_l) in enumerate(zip(train_losses, val_losses, test_losses)):
        writer.writerow([i + 1, train_l, val_l, test_l])
print(f"Loss history saved to: {csv_path}")

# Save the best model
torch.save({
    'model_state_dict': best_model_state,
    'label_shift': label_shift,
    'hyperparameters': best_hyperparams
}, 'best_raptor_model.pth')
print(f"\nModel saved to 'best_raptor_model.pth'")
print(f"Remember: To get original predictions, subtract {label_shift:.6f} from model outputs")